package com.gelerion.spark.streaming.book.source.tcp_web_server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;

public class JavaTcpClient {

    public static void main(String[] args) {
//        if (args.length < 2) return;

//        String hostname = args[0];
//        int port = Integer.parseInt(args[1]);

        try (Socket socket = new Socket("localhost", 9999)) {
            InputStream input = socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));

            String time = reader.readLine();
            System.out.println(time);
        } catch (UnknownHostException ex) {
            System.out.println("Server not found: " + ex.getMessage());
        } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
        }
    }

}
