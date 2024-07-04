/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

//命令行客户端类
import java.util.Scanner;
import client.Client;

public class CmdClient {
    private Client client;

    public CmdClient(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        printMenu();

        while (running) {
            System.out.print("> ");
            String input = scanner.nextLine();

            String[] tokens = input.split(" ");
            String command = tokens[0].toLowerCase();

            switch (command) {
                case "set":
                    handleSetCommand(tokens);
                    break;
                case "get":
                    handleGetCommand(tokens);
                    break;
                case "rm":
                    handleRmCommand(tokens);
                    break;
                case "quit":
                    running = false;
                    break;
                default:
                    System.out.println("ERROR: 无效的命令。");
            }
        }

        scanner.close();
    }

    private void printMenu() {
        System.out.println("===== 命令菜单 =====");
        System.out.println("set <key> <value> ");
        System.out.println("get <key> ");
        System.out.println("rm <key> ");
        System.out.println("quit ");
        System.out.println("====================");
    }

    private void handleSetCommand(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("ERROR: 用法：set <key> <value>");
            return;
        }

        String key = tokens[1];
        String value = tokens[2];
        client.set(key, value);
    }

    private void handleGetCommand(String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("ERROR: 用法：get <key>");
            return;
        }

        String key = tokens[1];
        String value = client.get(key);
//        if (value != null) {
//            System.out.println(value);
//        } else {
//            System.out.println("ERROR: 无法找到指定键。");
//        }
    }

    private void handleRmCommand(String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("ERROR: 用法：rm <key>");
            return;
        }

        String key = tokens[1];
        client.rm(key);
    }
}