package com.epokh.hdfs;

import com.epokh.hdfs.BatchView.BatchViewsHandler;
import com.epokh.hdfs.RealtimeView.RealtimeHandler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class serverMain{ 
    public static void main(String[] args) throws Exception {
        SpringApplication.run(serverMain.class, args);
        
        // BatchViewsHandler batchViewsHandler = new BatchViewsHandler();
        // batchViewsHandler.generate();

        Thread realtime = new Thread(new RealtimeHandler());
        realtime.start();

        Thread scheduler = new Thread(new Scheduler());
        scheduler.start();
    }
}
