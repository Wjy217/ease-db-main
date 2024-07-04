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

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";

    private static final int MEMORY_TABLE_THRESHOLD = 1000;   //内存表大小阈值：存储命令的最大数量
    private static final int FILE_SIZE_THRESHOLD = 1024 * 1024; //单个文件大小阈值

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
        this.reloadIndex();
        this.replayLog();
    }

    private void logToWAL(String commandType, String key, String value) {
        try {
            WALEntry entry = new WALEntry(commandType, key, value);
            byte[] entryBytes = JSONObject.toJSONBytes(entry);
            walFile.writeInt(entryBytes.length);
            walFile.write(entryBytes);
            walFile.getFD().sync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void replayLog() {
        try {
            walFile.seek(0); // Go to the start of the WAL file
            while (walFile.getFilePointer() < walFile.length()) {
                int entryLen = walFile.readInt();
                byte[] entryBytes = new byte[entryLen];
                walFile.read(entryBytes);
                WALEntry entry = JSONObject.parseObject(new String(entryBytes), WALEntry.class);

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

    //生成文件路径
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + currentFileIndex + TABLE;
    }


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


    //======将键值对存入内存表======
    //存储键值对数据。将键值对数据封装成SetCommand对象，并存入内存表(memTable)。
    //如果内存表大小超过阈值，则将内存表中的数据写入磁盘。然后将键值对数据写入磁盘文件，并更新索引。
    @Override
    public void set(String key, String value) {
        logToWAL("set", key, value);
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            indexLock.writeLock().lock();  //加锁
            memTable.put(key, command);  //将键值对存入内存

            // TODO://判断是否需要将内存表中的值写回table
            //如果内存表的大小超过阈值，则将内存表的数据写入磁盘
            if (memTable.size() >= MEMORY_TABLE_THRESHOLD) {
                writeToDisk();
            }

            rotateIfNeeded();

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

    //根据键从存储系统中检索对应的值
    //根据键获取对应的值。根据键查找索引(index)获取命令在文件中的位置和长度信息，然后从文件中读取命令数据，并解析为相应的命令对象。
    //如果是SetCommand对象，则返回其对应的值；如果是RmCommand对象，则返回null。
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
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    //删除键值对数据。将删除命令封装成 RmCommand对象，并存入内存表。
    // 如果内存表大小超过阈值，则将内存表中的数据写入磁盘。然后将删除命令写入磁盘文件，并更新索引。
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
                writeToDisk();
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

    //================写入磁盘，实现数据持久化==============
    //将内存表中的数据写入磁盘，同时更新索引
    private void writeToDisk() {
        try {
            //将内存表大小写入磁盘文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), memTable.size());
            for (Command command : memTable.values()) {
                //将命令对象转换为字节数组
                byte[] commandBytes = JSONObject.toJSONBytes(command);
                //将命令字节数组写入磁盘文件，获取写入位置
                int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
                CommandPos commandPos = new CommandPos(pos, commandBytes.length);  //创建命令位置对象
                index.put(command.getKey(), commandPos);   //将命令位置对象添加到索引中
            }
            memTable.clear();   //清空内存表中的数据，在将数据持久化到磁盘后，及时释放内存资源
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    //==============自动 rotate============
    //达到文件阈值之后，创建新文件，防止单个文件过大
    private void rotateIfNeeded() throws IOException {
        File currentFile = new File(this.genFilePath());
        //======判断文件是否超过文件阈值======
        if (currentFile.length() >= FILE_SIZE_THRESHOLD) {
            System.out.println("开始压缩");
            if (writerReader != null) {   //对象不为空则关闭句柄，确保之前的文件得到关闭
                writerReader.close();
            }
            deduplicateAndRotateFile();   //压缩的同时进行去重操作
            currentFileIndex++;   //增加当前文件索引，生成下一个文件路径
            writerReader = new RandomAccessFile(this.genFilePath(), RW_MODE);  //创建一个新的句柄，写入下一个文件
        }
    }

    //===================压缩（去重）===================
    //使用新的线程（多线程），异步执行文件去重。将内存中的数据按照分类写入对应的文件，并删除当前文件
    private void deduplicateAndRotateFile() {
        //创建一个新的线程，传入一个实现 Runnable接口的匿名内部类作为参数
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取当前文件的路径
                    String currentFilePath = dataDir + File.separator + NAME + currentFileIndex + TABLE;
                    //调用categorizeCommands方法对命令进行分类，得到一个 MAP对象   键：分类名称   值：分裂的命令列表
                    Map<String, List<Command>> categorizedCommands = categorizeCommands(memTable);
                    //遍历 categorizedCommands的每个键值对
                    for (Map.Entry<String, List<Command>> entry : categorizedCommands.entrySet()) {
                        String category = entry.getKey();   //获取键：分类名称
                        List<Command> commands = entry.getValue();   //获取值：命令列表
                        String categoryFilePath = dataDir + File.separator + category + TABLE;
                        //以读写模式打开分类文件
                        try (RandomAccessFile file = new RandomAccessFile(categoryFilePath, RW_MODE)) {
                            //遍历命令列表的每个元素
                            for (int i = 0; i < commands.size(); i++) {
                                Command command = commands.get(i);  //获取当前对象
                                byte[] commandBytes = JSONObject.toJSONBytes(command);

                                //将命令的长度写入文件中，以便后续读取时能够准确解析命令
                                RandomAccessFileUtil.writeInt(categoryFilePath, commandBytes.length);
                                RandomAccessFileUtil.write(categoryFilePath, commandBytes);
                            }
                        }
                    }
                    new File(currentFilePath).delete();   //删除当前文件，释放磁盘空间，完成 “去重 ”操作
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    //根据命令类型(SET/RM)将命令进行分类，存储到不同的文件中
    private Map<String, List<Command>> categorizeCommands(Map<String, Command> commands) {
        //创建一个用于存储分类后的 Map对象
        Map<String, List<Command>> categorized = new HashMap<>();
        //遍历每个对象
        for (Command command : commands.values()) {
            String category;
            //判断当前的命令对象类型
            if (command instanceof SetCommand) {
                category = "set";
            } else if (command instanceof RmCommand) {
                category = "rm";
            } else {
                category = "other";
            }
            //如果不包含当前分类，则添加一个空的命令列表
            if (!categorized.containsKey(category)) {
                categorized.put(category, new ArrayList<>());
            }
            categorized.get(category).add(command);   //添加到对应的命令列表中
        }
        return categorized;
    }

    //在 NormalStore 类中添加 replayLog 方法
    public void replayLog(String logFilePath) {
        try {
            RandomAccessFile logFile = new RandomAccessFile("../../../../../data/logfile", "r");

            while (logFile.getFilePointer() < logFile.length()) {
                int cmdLen = logFile.readInt(); // 读取命令长度
                byte[] commandBytes = new byte[cmdLen];
                logFile.read(commandBytes); // 读取命令数据
                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command command = CommandUtil.jsonToCommand(value); // 解析命令对象

                if (command instanceof SetCommand) {
                    SetCommand setCmd = (SetCommand) command;
                    set(setCmd.getKey(), setCmd.getValue()); // 执行 set 操作
                } else if (command instanceof RmCommand) {
                    RmCommand rmCmd = (RmCommand) command;
//                    remove(rmCmd.getKey()); // 执行 remove 操作
                }
                // 可以根据实际情况继续处理其他类型的命令
            }

            logFile.close(); // 关闭日志文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

