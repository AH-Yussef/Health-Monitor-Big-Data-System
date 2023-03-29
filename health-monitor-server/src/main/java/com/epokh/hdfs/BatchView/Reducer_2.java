package com.epokh.hdfs.BatchView;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer_2 extends Reducer<IntWritable,Text,NullWritable,GenericRecord> {
    public static final Schema MONTH_SCHEMA = new Schema.Parser().parse(
        "{\n" +
        "	\"type\":	\"record\",\n" +	
        "   \"namespace\":  \"EPOKH\",\n" +			
        "	\"name\":	\"year_report\",\n" +
        "	\"fields\":\n" + 
        "	[\n" + 
        "			{\"name\": \"month\",	\"type\": \"int\"},\n"+ 
        "			{\"name\": \"year\",	\"type\": \"int\"},\n"+ 

        "			{\"name\":	\"totalCpuUtil\", \"type\":	\"long\"},\n"+
        "			{\"name\": \"maxCpuUtil\",	\"type\": \"float\"},\n"+ 
        "			{\"name\": \"cpuPeakTime\",	\"type\": \"int\"},\n"+ 

        "			{\"name\":	\"totalRamUtil\", \"type\":	\"long\"},\n"+
        "			{\"name\":	\"maxRamUtil\", \"type\":	\"float\"},\n"+
        "			{\"name\":	\"ramPeakTime\", \"type\":	\"int\"},\n"+

        "			{\"name\":	\"totalDiskUtil\", \"type\": \"long\"},\n"+
        "			{\"name\":	\"maxDiskUtil\", \"type\":	\"float\"},\n"+
        "			{\"name\":	\"diskPeakTime\", \"type\":	\"int\"},\n"+

        "			{\"name\":	\"recordCount\", \"type\":	\"int\"},\n"+
        "			{\"name\":	\"msgCount\", \"type\":	\"long\"}\n"+
        "	]\n"+
        "}\n"
    );

    private NullWritable none;
    private	GenericRecord record = new GenericData.Record(MONTH_SCHEMA);
    private MultipleOutputs<NullWritable, GenericRecord> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
        String min_cpu_ram_disk_count[];
        int month, year, minOfMonth, temp, serviceId, recordCount = 0;
        long count, totalCount = 0;
        float cpuUtil = 0, ramUtil = 0, diskUtil = 0;
        float totalCpuUtil = 0, totalRamUtil = 0, totalDiskUtil = 0;
        float cpuUtilMax = 0, ramUtilMax = 0, diskUtilMax = 0;
        int cpuUtilPeak = 0, ramUtilPeak = 0, diskUtilPeak = 0;
        StringBuilder dest;

        temp = key.get();
        serviceId = (int)(temp % 10);
        temp /= 10;
        month = temp % 100;
        year = temp / 100;
        
        for (Text value : values) {
            min_cpu_ram_disk_count = value.toString().split(" ");

            minOfMonth = Integer.parseInt(min_cpu_ram_disk_count[0]);
            cpuUtil = Float.parseFloat(min_cpu_ram_disk_count[1]);        
            ramUtil = Float.parseFloat(min_cpu_ram_disk_count[2]);
            diskUtil = Float.parseFloat(min_cpu_ram_disk_count[3]);
            count = Long.parseLong(min_cpu_ram_disk_count[4]);

            totalCpuUtil += cpuUtil;
            totalRamUtil += ramUtil;
            totalDiskUtil += diskUtil;
            
            if (cpuUtil > cpuUtilMax) {
                cpuUtilMax = cpuUtil;
                cpuUtilPeak = minOfMonth;
            }
            if (ramUtil > ramUtilMax) {
                ramUtilMax = ramUtil;
                ramUtilPeak = minOfMonth;
            }
            if (diskUtil > diskUtilMax) {
                diskUtilMax = diskUtil;
                diskUtilPeak = minOfMonth;
            }

            totalCount += count;
            recordCount ++;
        }

        record.put("month", month);
        record.put("year", year);

        record.put("totalCpuUtil", totalCpuUtil);
        record.put("maxCpuUtil", cpuUtilMax);
        record.put("cpuPeakTime", cpuUtilPeak);

        record.put("totalRamUtil", totalRamUtil);
        record.put("maxRamUtil", ramUtilMax);
        record.put("ramPeakTime", ramUtilPeak);

        record.put("totalDiskUtil", totalDiskUtil);
        record.put("maxDiskUtil", diskUtilMax);
        record.put("diskPeakTime", diskUtilPeak);

        record.put("recordCount", recordCount);
        record.put("msgCount", totalCount);

        dest = new StringBuilder();
        dest.append("report_service-").append(Integer.toString(serviceId));

        multipleOutputs.write(none, record, dest.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
} 
   