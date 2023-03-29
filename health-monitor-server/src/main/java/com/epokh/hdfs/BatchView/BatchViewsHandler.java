package com.epokh.hdfs.BatchView;

import java.io.IOException;

import com.epokh.hdfs.Shared;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

public class BatchViewsHandler {    
    public void generate() throws Exception 
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoopmaster:9000");

        reset(conf);

        boolean success_1 = job_1(conf);
        System.out.println(success_1);
        
        if(success_1) {
            boolean success_2 = job_2(conf);
            System.out.println(success_2);

            if(success_2) renameBatchViews(conf);
        }
    }

    private void reset(Configuration configuration) throws IOException {
        FileSystem hdfs = FileSystem.get(configuration);
        // hdfs.delete(new Path("/batch_views"), true);
        // hdfs.delete(new Path("/years_reports"), true);


        hdfs.delete(new Path(Shared.bv_mon_out), true);
        hdfs.delete(new Path(Shared.bv_year_out), true);

        hdfs.close();
    }

    private boolean job_1(Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(configuration, "job_1");
            
        job.setJarByClass(BatchViewsHandler.class);
        job.setMapperClass(Mapper_1.class);
        job.setReducerClass(Reducer_1.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path("/health_reports"));
        // FileOutputFormat.setOutputPath(job, new Path("/batch_views"));
        FileOutputFormat.setOutputPath(job, new Path(Shared.bv_mon_out));


        AvroParquetOutputFormat.setSchema(job, Reducer_1.MINUTE_SCHEMA);

        LazyOutputFormat.setOutputFormatClass(job, AvroParquetOutputFormat.class);

        MultipleOutputs.addNamedOutput(job,"service1", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"service2", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"service3", TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"service4", TextOutputFormat.class,Text.class,Text.class);
            
        boolean status = job.waitForCompletion(true);
        return status;
    }

    private boolean job_2(Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {
        Job job2 = Job.getInstance(configuration, "job_2");

        job2.setJarByClass(BatchViewsHandler.class);
        job2.setMapperClass(Mapper_2.class);
        job2.setReducerClass(Reducer_2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Group.class);
        job2.setInputFormatClass(ExampleInputFormat.class);
        job2.setOutputFormatClass(AvroParquetOutputFormat.class);

        job2.setNumReduceTasks(1);

        // FileInputFormat.addInputPath(job2, new Path("/batch_views/**"));
        // FileOutputFormat.setOutputPath(job2, new Path("/years_reports"));

        FileInputFormat.addInputPath(job2, new Path(Shared.bv_mon_out +"/**"));
        FileOutputFormat.setOutputPath(job2, new Path(Shared.bv_year_out));

        
        LazyOutputFormat.setOutputFormatClass(job2, AvroParquetOutputFormat.class);

        AvroParquetOutputFormat.setSchema(job2, Reducer_2.MONTH_SCHEMA);

        return job2.waitForCompletion(true);
    }

    private void renameBatchViews(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        StringBuilder parent = new StringBuilder();

        for(int i = 1; i <= 4; i++) {
            parent.setLength(0);
            // parent.append("hdfs://hadoopmaster:9000/batch_views/service").append(i).append("/");
            parent.append("hdfs://hadoopmaster:9000"+Shared.bv_mon_out+"/service").append(i).append("/");

            renameFiles(parent.toString(), fs);
        }

        // renameFiles("hdfs://hadoopmaster:9000/years_reports/", fs);
        renameFiles("hdfs://hadoopmaster:9000"+Shared.bv_year_out+"/", fs);
        fs.close();
    }

    private void renameFiles(String parent, FileSystem fs) throws IllegalArgumentException, IOException {
        for (FileStatus fileStatus : fs.listStatus(new Path(parent))) {
            String fileName = fileStatus.getPath().getName();
            String[] parts = fileName.split("-");
            if(parts.length <= 1) continue;
            String newName = parts[0] + "-" + parts[1];
            fs.rename(new Path(parent + fileName), new Path(parent + newName+".parquet"));
        }
    }
}
