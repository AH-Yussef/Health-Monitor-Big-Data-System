package com.epokh.hdfs;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class test {
    public static void main(String[] args) throws IllegalArgumentException, IOException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopmaster:9000");
        FileSystem fs = FileSystem.get(config);

        for(int i = 21; i <=50; i++) {
            fs.delete(new Path("/health_reports/health_"+i+".csv"));
        }

        fs.close();
    }
}
