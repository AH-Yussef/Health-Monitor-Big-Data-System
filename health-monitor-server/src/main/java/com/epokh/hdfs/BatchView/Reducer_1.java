package com.epokh.hdfs.BatchView;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.joda.time.DateTime;

public class Reducer_1 extends Reducer<LongWritable,Text,NullWritable,GenericRecord> {
    public	static final Schema MINUTE_SCHEMA = new Schema.Parser().parse(
        "{\n" +
        "	\"type\":	\"record\",\n" +	
        "   \"namespace\":  \"EPOKH\",\n" +			
        "	\"name\":	\"month_report\",\n" +
        "	\"fields\":\n" + 
        "	[\n" + 
        "			{\"name\": \"minOfMonth\",	\"type\": \"int\"},\n"+ 
        "			{\"name\": \"cpuUtil\",	\"type\": \"float\"},\n"+ 
        "			{\"name\":	\"ramUtil\", \"type\":	\"float\"},\n"+
        "			{\"name\":	\"diskUtil\", \"type\":	\"float\"},\n"+
        "			{\"name\":	\"msgCount\", \"type\":	\"long\"}\n"+
        "	]\n"+
        "}\n"
    );

    private NullWritable none;
    private GenericRecord record = new GenericData.Record(MINUTE_SCHEMA);
    private MultipleOutputs<NullWritable, GenericRecord> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
        String cpu_ram_disk[];
        long timestamp;
        int serviceId;
        float cpuUtil = 0, ramUtil = 0, diskUtil = 0;
        float cpuUtilTotal = 0, ramUtilTotal = 0, diskUtilTotal = 0;
        float ramTotal = 0, diskTotal = 0;
        float ramFree = 0, diskFree = 0;
        long count = 0;

        StringBuilder dest;

        serviceId = (int)(key.get() % 10);
        timestamp = key.get() / 10;
        DateTime msgDT = new DateTime(timestamp * 1000L);
        int year = msgDT.getYear();
        int month = msgDT.getMonthOfYear();
        int day = msgDT.getDayOfMonth();
        int minOfDay = msgDT.getMinuteOfDay();
        int minOfMonth = (day-1)*24*60 + minOfDay;
    
        for (Text value : values) {
            count ++;

            cpu_ram_disk = value.toString().split(" ");

            cpuUtil = Float.parseFloat(cpu_ram_disk[0]);

            ramTotal = Float.parseFloat(cpu_ram_disk[1]);
            ramFree = Float.parseFloat(cpu_ram_disk[2]);
            ramUtil = (ramTotal - ramFree) / ramTotal;
    
            diskTotal = Float.parseFloat(cpu_ram_disk[3]);
            diskFree = Float.parseFloat(cpu_ram_disk[4]);
            diskUtil = (diskTotal - diskFree) / diskTotal;
            
            cpuUtilTotal += cpuUtil;
            ramUtilTotal += ramUtil;
            diskUtilTotal += diskUtil;
        }


        record.put("minOfMonth", minOfMonth);
        record.put("cpuUtil", cpuUtilTotal / count);
        record.put("ramUtil", ramUtilTotal / count);
        record.put("diskUtil", diskUtilTotal / count);
        record.put("msgCount", count);
                    
        dest = new StringBuilder();
        dest.append("service").append(Integer.toString(serviceId)).append("/");
        dest.append(Integer.toString(month)).append("-").append(year);

        multipleOutputs.write(none, record, dest.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
