/*
 *@Type LoggerUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 01:34
 * @version
 */
package utils;

import org.slf4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class LoggerUtil {
    private static final String LOG_FILE_PATH = "D:\\ideaFiles\\easy-db-main";

    public static void debug(Logger logger, String format, Object... arguments) {
        if (logger.isDebugEnabled()) {
            logger.debug(format, arguments);
        }
    }

    public static void info(Logger logger, String format, Object... arguments) {
        if (logger.isInfoEnabled()) {
            logger.info(format, arguments);
        }
    }

    public static void error(Logger logger, Throwable t, String format, Object... arguments) {
        if (logger.isErrorEnabled()) {
            logger.error(format, arguments, t);
        }
    }

    public static void replayLog() {
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 解析日志记录
                LogRecord record = parseLogRecord(line);
                if (record != null) {
                    // 根据日志记录进行相应操作
                    executeOperation(record);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static LogRecord parseLogRecord(String line) {
        // 解析日志记录的逻辑
        // 根据实际的日志格式进行解析，构建对应的LogRecord对象
        // 返回解析后的LogRecord对象，如果解析失败返回null
        return null;
    }

    private static void executeOperation(LogRecord record) {
        // 根据日志记录执行相应的操作
        // 可以根据操作类型进行不同的处理，例如插入、更新、删除等操作
        // 将操作应用到内存数据结构，保持内存与磁盘数据的一致性
        // ...
    }
}

class LogRecord {
    // 日志记录的数据结构
    // 根据实际的日志格式定义相应的字段和方法
    // 可能包括操作类型、目标对象、数据变化等信息
}