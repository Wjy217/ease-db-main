/*
 *@Type ServerController.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 12:20
 * @version
 */
package controller;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.NormalStore;
import service.Store;
import utils.LoggerUtil;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

@Setter
@Getter
public class SocketServerController implements Controller {

    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerController.class);
    private final String logFormat = "[SocketServerController]{}: {}";
    private String host;   //主机名
    private int port;   //端口
    private Store store;   //存储引擎

    public SocketServerController(String host, int port, Store store) {
        this.host = host;
        this.port = port;
        this.store = store;
    }

    @Override
    public void set(String key, String value) {
        store.set(key,value);
    }

    @Override
    public String get(String key) {
        return store.get(key);
    }

    @Override
    public void rm(String key) {
        store.rm(key);
    }

    @Override
    public void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            //输出日志信息
            LoggerUtil.info(LOGGER, logFormat,"startServer","服务器已启动，等待连接...");

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    LoggerUtil.info(LOGGER, logFormat,"startServer","新用户已连接");
                    //每有客户端连接时，创建一个新的线程
                    new Thread(new SocketServerHandler(socket, store)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
