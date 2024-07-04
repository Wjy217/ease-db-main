/*
 *@Type SocketServerHandler.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 12:50
 * @version
 */
package controller;

import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import service.NormalStore;
import service.Store;
import utils.LoggerUtil;

import java.io.*;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//处理 Socket连接的服务器端处理器，它接收客户端发送的命令，根据命令类型执行相应的操作，并将结果返回给客户端
public class SocketServerHandler implements Runnable {
    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerHandler.class);
    private Socket socket;
    private Store store;

    public SocketServerHandler(Socket socket, Store store) {
        this.socket = socket;
        this.store = store;
    }

    @Override
    public void run() {
        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            // 接收序列化对象，该对象包含了客户端发送的命令信息
            ActionDTO dto = (ActionDTO) ois.readObject();
            LoggerUtil.debug(LOGGER, "[SocketServerHandler][ActionDTO]: {}", dto.toString());
            System.out.println("" + dto.toString());

            // 处理命令的逻辑
            switch (dto.getType()) {
                case GET:
                    String value = this.store.get(dto.getKey());
                    LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "获取命令的响应" + dto.toString());
                    RespDTO respGet = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
                    oos.writeObject(respGet);
                    oos.flush();
                    break;
                case SET:
                    this.store.set(dto.getKey(), dto.getValue());
                    LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "设置命令的响应" + dto.toString());
                    RespDTO respSet = new RespDTO(RespStatusTypeEnum.SUCCESS, dto.getValue());
                    oos.writeObject(respSet);
                    oos.flush();
                    break;
                case RM:
                    this.store.rm(dto.getKey());
                    LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "删除命令的响应" + dto.toString());
                    RespDTO respRm = new RespDTO(RespStatusTypeEnum.SUCCESS, "删除成功");
                    oos.writeObject(respRm);
                    oos.flush();
                    break;
                default:
                    break;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                socket.close();   //关闭连接
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
