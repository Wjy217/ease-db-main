package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dto.WALEntry;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.Store;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPOutputStream;

public class NormalStore implements Store {

    public static final String TABLE = ".table";   //文件扩展名
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    private static final int MEMORY_TABLE_THRESHOLD = 4;   //内存表大小阈值：存储命令的最大数量
    private static final int FILE_SIZE_THRESHOLD = 5; //单个文件大小阈值
    private static final int FILE_ENTRY_THRESHOLD = 5;   //单个文件中键值对的最大数量

    private TreeMap<String, Command> memTable;   //存储命令的内存表
    private HashMap<String, CommandPos> index;   //哈希索引，存的是数据长度和偏移量
    private final String dataDir;   //数据目录
    private final ReadWriteLock indexLock;   //读写锁，支持多线程，并发安全写入
    private RandomAccessFile writerReader;   //暂存数据的日志句柄
    private int currentFileIndex = 0;   //当前文件的索引
    private RandomAccessFile walFile;

    public NormalStore(String dataDir) throws FileNotFoundException {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<>();
        this.index = new HashMap<>();
        this.writerReader = new RandomAccessFile(this.genFilePath(), RW_MODE);
        this.walFile = new RandomAccessFile(this.dataDir + File.separator + "wal.log", RW_MODE);

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "数据目录不存在，正在创建...");
            file.mkdirs();
        }
        //重新启动时，先恢复索引，再回放日志文件
        this.reloadIndex();
        this.replayLog();
    }

    //将操作写入 WAL日志文件中
    private void logToWAL(String commandType, String key, String value) {
        try {
            // WALEntry包含三个属性：命令、key、value
            WALEntry entry = new WALEntry(commandType, key, value);
            byte[] entryBytes = JSONObject.toJSONBytes(entry);
            walFile.writeInt(entryBytes.length);
            walFile.write(entryBytes);
            walFile.getFD().sync();   //每次写入 WAL文件的数据及时刷新到磁盘
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //逐一回放日志文件
    public void replayLog() {
        try {
            walFile.seek(0);   //将文件指针移动到文件开头
            while (walFile.getFilePointer() < walFile.length()) {
                int entryLen = walFile.readInt();
                byte[] entryBytes = new byte[entryLen];
                walFile.read(entryBytes);
                WALEntry entry = JSONObject.parseObject(new String(entryBytes), WALEntry.class);
                //根据操作类型执行对应的操作
                if ("set".equals(entry.getCommandType())) {
                    set(entry.getKey(), entry.getValue());
                } else if ("rm".equals(entry.getCommandType())) {
                    rm(entry.getKey());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //回放索引
    public void reloadIndex() {
        try {
            for (int i = 0; i <= currentFileIndex; i++) {
                String filePath = this.dataDir + File.separator + NAME + i + TABLE;
                RandomAccessFile file = new RandomAccessFile(filePath, RW_MODE);
                FileChannel channel = file.getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);

                while (true) {
                    int bytesRead = channel.read(buffer);
                    if (bytesRead == -1) {
                        break;
                    }
                    buffer.flip();
                    while (buffer.remaining() > 4) {
                        int cmdLen = buffer.getInt();
                        if (buffer.remaining() >= cmdLen) {
                            byte[] bytes = new byte[cmdLen];
                            buffer.get(bytes);
                            String jsonString = new String(bytes, StandardCharsets.UTF_8);
                            JSONObject value = JSON.parseObject(jsonString);
                            Command command = CommandUtil.jsonToCommand(value);
                            if (command != null) {
                                long startPos = buffer.position() - cmdLen - 4;
                                CommandPos cmdPos = new CommandPos((int) startPos, cmdLen);
                                index.put(command.getKey(), cmdPos);
                            }
                        }
                    }
                    buffer.compact();
                }
                file.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "重新加载索引: " + index.toString());
    }

    //存储键值对数据。将键值对数据封装成 SetCommand对象，并存入内存表(memTable)。
    @Override
    public void set(String key, String value) {
        logToWAL("set", key, value);   // 1.在操作之前先写入日志文件
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // TODO:// 2.先写内存表，内存表达到一定阀值再写进磁盘
            indexLock.writeLock().lock();  //加锁
            memTable.put(key, command);  //将键值对存入内存

            // TODO:// 3.判断是否需要将内存表中的值写回table
            //如果内存表的大小超过阈值，则将内存表的数据写入磁盘
            if (memTable.size() >= MEMORY_TABLE_THRESHOLD) {
                writeToDisk(this.setFilePath());
                writeToDisk(this.genFilePath());
            }
            rotateIfNeeded();   //自动 rotate
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    //根据键获取对应的值。根据键查找索引(index)获取命令在文件中的位置和长度信息，然后从文件中读取命令数据，并解析为相应的命令对象。
    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            //获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
            //如果是S etCommand对象，则返回其对应的值；如果是 RmCommand对象，则返回 null。
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();   //释放读锁
        }
        return null;
    }

    //删除键值对数据。将删除命令封装成 RmCommand对象，并存入内存表。
    @Override
    public void rm(String key) {
        logToWAL("rm", key, null);
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            indexLock.writeLock().lock();  //加锁
            memTable.put(key, command);   //将键值对存入内存表

            // TODO://判断是否需要将内存表中的值写回table
            //如果内存表的大小超过阈值，则将内存表中的数据写入磁盘
            if (memTable.size() >= MEMORY_TABLE_THRESHOLD) {
                writeToDisk(this.rmFilePath());
                writeToDisk(this.genFilePath());
            }
            rotateIfNeeded();   //文件切换
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();   //释放写锁
        }
    }

    //关闭存储对象
    @Override
    public void close() throws IOException {
        writerReader.close();
    }

    //将内存表中的数据写入磁盘，同时更新索引
    private void writeToDisk(String path) {
        try {
            //将内存表大小写入磁盘文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), memTable.size());
            for (Command command : memTable.values()) {
                //将命令对象转换为字节数组
                byte[] commandBytes = JSONObject.toJSONBytes(command);
                //将命令字节数组写入磁盘文件，获取写入位置
                int pos = RandomAccessFileUtil.write(path, commandBytes);
                CommandPos commandPos = new CommandPos(pos, commandBytes.length);  //创建命令位置对象
                index.put(command.getKey(), commandPos);   //将命令位置对象添加到索引中
            }
            memTable.clear();   //清空内存表中的数据，在将数据持久化到磁盘后，及时释放内存资源
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    //达到文件阈值之后，创建新文件，防止单个文件过大
    private void rotateIfNeeded() throws IOException {
        File currentFile = new File(this.genFilePath());
        //判断文件是否达到阈值
        if (currentFile.length() >= FILE_SIZE_THRESHOLD) {
            System.out.println("开始压缩");
            if (writerReader != null) {   //对象不为空则关闭句柄，确保之前的文件得到关闭
                writerReader.close();
            }
            rmDuplicate();   //文件切换的同时进行去重操作
            currentFileIndex++;   //增加当前文件索引，生成下一个文件路径
            String newFilePath = this.genFilePath();
            File newFile = new File(newFilePath);
            if (!newFile.exists()){
                newFile.createNewFile();
            }
            writerReader = new RandomAccessFile(this.genFilePath(), RW_MODE);  //创建一个新的句柄，写入下一个文件
        }
    }

    // 7.19新
    private void rmDuplicate() {
        new Thread(new Runnable() {   //新建一个线程
            @Override
            public void run() {
                try {
                    String currentFilePath = dataDir + File.separator + NAME + currentFileIndex + TABLE;
                    //调用 duplicateCommands()方法对内存表中的命令进行去重操作，返回去重后的命令集合 uniqueCommands
                    Map<String, Command> uniqueCommands = duplicateCommands(memTable);
                    try (RandomAccessFile file = new RandomAccessFile(currentFilePath, RW_MODE)) {
                        //将去重后的命令转换为字节数组
                        for (Command command : uniqueCommands.values()) {
                            byte[] commandBytes = JSONObject.toJSONBytes(command);
                            RandomAccessFileUtil.writeInt(currentFilePath, commandBytes.length);  //将字节数组的长度写入文件
                            RandomAccessFileUtil.write(currentFilePath, commandBytes);  //将字节数组写入文件
                        }
                    }
                    new File(currentFilePath).delete();   //删除当前文件
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    //命令的去重操作
    private Map<String, Command> duplicateCommands(Map<String, Command> commands) {
        Map<String, Command> uniqueCommands = new HashMap<>();   //用于存储去重后的命令
        //遍历命令，并只保留最新的命令
        for (Command command : commands.values()) {
            uniqueCommands.put(command.getKey(), command);  //将每个命令根据其键添加到哈希映射中，只保留最新的命令
        }
        return uniqueCommands;   //返回去重后的命令
    }

    //生成文件路径
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + currentFileIndex + TABLE;
    }

    public String setFilePath() {
        return this.dataDir + File.separator + "set"  + TABLE;
    }

    public String rmFilePath() {
        return this.dataDir + File.separator + "rm"  + TABLE;
    }
}

