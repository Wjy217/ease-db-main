/*
 *@Type SocketServerUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:08
 * @version
 */
package example;

import controller.SocketServerController;
import service.NormalStore;
import service.Store;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServerUsage {
    public static void main(String[] args) throws FileNotFoundException {
        String host = "localhost";
        int port = 12345;
        String dataDir = "data" + File.separator;
        Store store = new NormalStore(dataDir);
        SocketServerController controller = new SocketServerController(host, port, store);
        controller.startServer();
    }
}
