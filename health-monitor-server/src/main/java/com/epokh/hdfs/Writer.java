package com.epokh.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Writer implements Runnable{
    private final String HDFS_ROOT = "/health_logs/";
    private FileSystem hdfs;

    public Writer() {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopmaster:9000");
        try {
            hdfs = FileSystem.get(config);
        } catch(IOException e) {
            System.out.println("I/O error: " + e.getMessage());
        }
    }

    public void appendToHDFSFile(String[] contents) throws IOException {
        Path targetPath = getLogFilePath();

        FSDataOutputStream fsDataOutputStream;
        if(!hdfs.exists(targetPath)) {
            fsDataOutputStream = hdfs.create(targetPath, true);
        }
        else fsDataOutputStream = hdfs.append(targetPath);
        
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        for(int i = 0; i < contents.length; i++){
            bufferedWriter.write(contents[i]);
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
    }

    private Path getLogFilePath() {
        SimpleDateFormat newDateFormat = new SimpleDateFormat("dd_MM_yyyy");
        String stringOfDate = newDateFormat.format(new Date());
        String fileName = stringOfDate + ".log";
        Path path = new Path(HDFS_ROOT + fileName);
        return path;
    }

    public void shutdown() throws IOException {
        hdfs.close();
    }

    @Override
    public void run() {
        System.out.println("Starting Writer...");

        FileWriter out = null;
        String[] batch = new String[1024];
        int msgCount = 0;
        int batches = 0;
        long totalWriteTime = 0;
        long totalEndEndTime = 0;

        while(true) {
            if(Shared.queue.isEmpty()) continue;
            byte[] payload = Shared.queue.poll().getData();
            String jsonMsg = new String(payload, StandardCharsets.UTF_8); 
            batch[msgCount++] = jsonMsg;

            if(msgCount == 1024) {
                try {
                    long start = System.nanoTime();
                    appendToHDFSFile(batch);
                    long end = System.nanoTime();
                    double writeTime =(end - start) * Math.pow(10, -6);
                    totalWriteTime += writeTime;

                    double endEndTime = (end - Shared.recvTime.get(batches)) * Math.pow(10, -6);
                    totalEndEndTime += endEndTime;
                    batches ++;

                    System.out.println("batch: "+batches);        try {
                        out = new FileWriter("analysis.txt", true);
                        out.write(batches+" "+writeTime+" "+totalWriteTime/(double)batches+" "+endEndTime+" "+totalEndEndTime/batches);
                        out.write("\n");
                        out.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                msgCount = 0;
            } 
        }
    }
}
