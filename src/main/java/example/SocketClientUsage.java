/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.CmdClient;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.run();
        client.set("wjy11", "for test");
        client.get("wjy11");
        client.rm("wjy11");
        client.get("wjy11");
    }
}