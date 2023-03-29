package com.epokh.hdfs;
import java.io.*;
import java.net.*;

public class HdfsClient {
    private final String hostname = "hadoopmaster";
    private final int port = 9999;
    private InetAddress address;
    private DatagramSocket socket;
    
    public HdfsClient() {
        try {
            address = InetAddress.getByName(hostname);
            socket = new DatagramSocket();
        } catch (SocketException e) {
            System.out.println("socket error " + e.getMessage());
        } catch (UnknownHostException e) {
            System.out.println("host error " + e.getMessage());
        }
    }

    private void sendMsg(String msg) {
        try {
            byte[] buffer = msg.getBytes();
            DatagramPacket healthMsg = new DatagramPacket(buffer, buffer.length, address, port);
            socket.send(healthMsg);
        } catch (IOException e) {
            System.out.println("I/O error " + e.getMessage());
        } 
    }

    public void getAllMessages(String path) throws IOException, InterruptedException {
        File file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;

        while ((line = br.readLine()) != null) {
            sendMsg(line);
            Thread.sleep(1000);
        }
        br.close();
    }
}
