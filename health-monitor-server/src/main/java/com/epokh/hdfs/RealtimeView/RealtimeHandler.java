package com.epokh.hdfs.RealtimeView;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class RealtimeHandler implements Runnable{
    public void generate() throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount").master("local[2]")
                .getOrCreate();
                
        Dataset<String> lines = spark
        .readStream()
        .format("socket")
        .option("host", "hadoopmaster")
        .option("port", 9999)
        .load()
        .as(Encoders.STRING());

        Encoder<HealthMsg> healthMsgEncoder = Encoders.bean(HealthMsg.class);
        Dataset<HealthMsg> healthMsgs = lines.map(
            (MapFunction<String, HealthMsg>) line -> {
                String[] parts = line.split(",");
                HealthMsg msg = new HealthMsg();
                msg.setServiceId(Integer.parseInt((parts[0].split("-"))[1]));
                msg.setTimestamp(Long.parseLong(parts[1]));
                msg.setCpuUtil(Float.parseFloat(parts[2]));

                float ramTotal = Float.parseFloat(parts[3]);
                float ramFree = Float.parseFloat(parts[4]);
                float ramUtil = (ramTotal - ramFree) / ramTotal;
                msg.setRamUtil(ramUtil);

                float diskTotal = Float.parseFloat(parts[5]);
                float diskFree = Float.parseFloat(parts[6]);
                float diskUtil = (diskTotal - diskFree) / diskTotal;
                msg.setDiskUtil(diskUtil);

                return msg;
            },
            healthMsgEncoder);
        
        healthMsgs
        .writeStream()
        .format("parquet")
        .outputMode("append")
        .option("path","hdfs://hadoopmaster:9000/realtime")
        .option("checkpointLocation","hdfs://hadoopmaster:9000/checkpoint")
        .start()
        .awaitTermination();
    }

    private void reset() throws IOException {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopmaster:9000");
        FileSystem hdfs = FileSystem.get(config);


        hdfs.delete(new Path("/realtime"), true);
        hdfs.delete(new Path("/checkpoint"), true);

        hdfs.close();
    }

    @Override
    public void run() {
        try {
            // reset();
            // generate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}