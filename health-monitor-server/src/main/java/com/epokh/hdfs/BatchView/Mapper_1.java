package com.epokh.hdfs.BatchView;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_1 extends Mapper<Object, Text, LongWritable, Text>
{
  private LongWritable timestamp_id = new LongWritable();
  private Text cpu_ram_disk = new Text();
  private String[] words;
  
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
  {
    words = value.toString().split(",");
    long timestamp = Long.parseLong(words[1]);
    int serviceId = Integer.parseInt(words[0].split("-")[1]);
    timestamp = (timestamp / 60) * 60; //remove seconds

    StringBuilder output = new StringBuilder();
    output.append(words[2]).append(" ");
    output.append(words[3]).append(" ").append(words[4]).append(" ");
    output.append(words[5]).append(" ").append(words[6]).append(" ");

    timestamp_id.set((timestamp * 10) + serviceId);
    cpu_ram_disk.set(output.toString());

    try {
        context.write(timestamp_id, cpu_ram_disk);
    }catch (Exception e){
        e.printStackTrace();
    }
  }
}
