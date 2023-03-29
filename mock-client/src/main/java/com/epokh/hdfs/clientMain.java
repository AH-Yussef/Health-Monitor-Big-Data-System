package com.epokh.hdfs;

import java.io.IOException;

public class clientMain 
{
    public static void main( String[] args ) throws InterruptedException
    {
        String filePath = "/media/sf_shared/health_logs/health_0.log";
        HdfsClient hdfsClient = new HdfsClient();
        
        try {
            hdfsClient.getAllMessages(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    
        System.out.println("done");
    }
}
