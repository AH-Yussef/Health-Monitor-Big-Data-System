package com.epokh.hdfs.BatchView;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.Group;

public class Mapper_2 extends Mapper<Object, Group, IntWritable, Text> {
    private IntWritable year_month_id = new IntWritable();
    private Text out = new Text();

    private String minOfMonth, cpuUtil, ramUtil, diskUtil, msgCount;
    private String[] cols;
    private StringBuilder buffer = new StringBuilder();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String parentName = fileSplit.getPath().getParent().getName();        
        String[] parts = fileName.split("-");
        int fileYear = Integer.parseInt(parts[1]);
        int fileMonth = Integer.parseInt(parts[0]);
        int serviceId = Integer.parseInt(parentName.substring(parentName.length() -1));
        year_month_id.set((fileYear*100 + fileMonth)*10 + serviceId);
    }
    
    public void map(Object key, Group value, Context context) throws IOException, InterruptedException 
    {
    cols = value.toString().split("\n");
    minOfMonth = cols[0].split(": ")[1];
    cpuUtil = cols[1].split(": ")[1];
    ramUtil = cols[2].split(": ")[1];
    diskUtil = cols[3].split(": ")[1];
    msgCount = cols[4].split(": ")[1];

    buffer.setLength(0);
    buffer.append(minOfMonth).append(" ");
    buffer.append(cpuUtil).append(" ");
    buffer.append(ramUtil).append(" ");
    buffer.append(diskUtil).append(" ");
    buffer.append(msgCount);
        
    out.set(buffer.toString());

    try {
        context.write(year_month_id, out);
    }catch (Exception e){
        e.printStackTrace();
    }
    }
}