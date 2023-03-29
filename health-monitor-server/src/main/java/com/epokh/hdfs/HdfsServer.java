package com.epokh.hdfs;
import java.io.*;
import java.net.*;

public class HdfsServer {
    private DatagramSocket socket;
    private final int port = 3500;
    
    public HdfsServer() {
        try {
            socket = new DatagramSocket(port);
            Thread thread = new Thread(new Writer());
            thread.start();
        } catch (SocketException e) {
            System.out.println("socket error " + e.getMessage());
        }
    }

    public void service() {
        int msgCount = 0;

        while(true) {
            try {
            byte[] buffer = new byte[1024];
            DatagramPacket request = new DatagramPacket(buffer, buffer.length);
            socket.receive(request);
            //recv time
            msgCount ++;
            if(msgCount == 1) 
                Shared.recvTime.add(System.nanoTime());
            if(msgCount == 1024) msgCount = 0;

            Shared.queue.add(request);

            } catch (IOException e) {
                System.out.println("I/O error " + e.getMessage());
            }
        }
    }
}
