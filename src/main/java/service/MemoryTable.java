package service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import model.command.CommandPos;
import model.command.SetCommand;
import utils.RandomAccessFileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
public class MemoryTable {
    @Setter
    @Getter
    private Map<String, String> table;
    private int threshold;
    private String filePath; // 文件路径，用于写入磁盘
    public static final String DISK_TABLE_SUFFIX = ".txt";
    private final String dataDir;
    //使用时间戳生成文件名
    public String generateTimestampFileName() {
        long timestamp = Instant.now().toEpochMilli();
        return timestamp + DISK_TABLE_SUFFIX;
    }
    public String genDiskFilePath() {
        String fileName = generateTimestampFileName();
        return this.dataDir + File.separator + fileName;
    }

    public MemoryTable(int threshold, String dataDir) {
        this.dataDir = dataDir;
        this.table = new TreeMap<>();
        this.threshold = threshold;
    }

    public void put(String key, String value) {
        table.put(key, value);
    }

    public boolean shouldFlushToDisk() {
        return table.size() >= threshold;
    }

    public void clear() {
        table.clear();
    }
    // 将数据写入磁盘，实现去重和阈值控制
    public boolean flushToDisk() {
        if (table.size() >= threshold) {
            byte[] serializedData = serializeTable();
            boolean success = RandomAccessFileUtil.write(genDiskFilePath(), serializedData) != -1;
            if (success) {
                table.clear(); // 写入成功后清空内存中的数据
            }
            return success;
        }
        return true; // 内存中的数据未达到阈值，无需写入磁盘
    }
    // 将表格数据序列化为字节数组
    private byte[] serializeTable() {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, String> entry : table.entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        String jsonString = json.toJSONString(); // 将 JSONObject 转换为 JSON 字符串
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }
}
