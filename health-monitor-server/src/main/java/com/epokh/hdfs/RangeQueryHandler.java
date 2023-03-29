package com.epokh.hdfs;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.epokh.hdfs.Query.BV_QueryInput;
import com.epokh.hdfs.Query.BV_RangeQuery;
import com.epokh.hdfs.Query.RTV_RangeQuery;
import com.epokh.hdfs.Query.RangeQueryIntermmediateResult;
import com.epokh.hdfs.Query.RangeQueryResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class RangeQueryHandler {
    private SparkSession spark;
    private FileSystem fs1, fs2, fs3, fs4;

    public RangeQueryHandler() throws IOException{
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopmaster:9000");
        
        spark = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate();

        fs1 = FileSystem.get(config);
        fs2 = FileSystem.get(config);
        fs3 = FileSystem.get(config);
        fs4 = FileSystem.get(config);
    }

    public HashMap<String, String> getInfo(String startStr, String endStr) throws IOException, ClassNotFoundException, SQLException, ParseException, InterruptedException, ExecutionException {        
        Date sdt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(startStr);
        Date edt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(endStr);

        DateTime start = new DateTime(sdt);
        DateTime end = new DateTime(edt);

        long stt = start.getMillis() / 1000L;
        long ett = end.getMillis() / 1000L;

        BV_QueryInput input1 = createInput(1, fs1, start, end);
        BV_QueryInput input2 = createInput(2, fs2, start, end);
        BV_QueryInput input3 = createInput(3, fs3, start, end);
        BV_QueryInput input4 = createInput(4, fs4, start, end);


        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Future<RangeQueryIntermmediateResult> service1 = executorService.submit(new BV_RangeQuery(input1));
        Future<RangeQueryIntermmediateResult> service2 = executorService.submit(new BV_RangeQuery(input2));
        Future<RangeQueryIntermmediateResult> service3 = executorService.submit(new BV_RangeQuery(input3));
        Future<RangeQueryIntermmediateResult> service4 = executorService.submit(new BV_RangeQuery(input4));
        
        Future<RangeQueryIntermmediateResult[]> realtTime = executorService.submit(new RTV_RangeQuery(spark, stt, ett));
        // wait until result will be ready
        RangeQueryIntermmediateResult rsBv1 = service1.get();
        RangeQueryIntermmediateResult rsBv2 = service2.get();
        RangeQueryIntermmediateResult rsBv3 = service3.get();
        RangeQueryIntermmediateResult rsBv4 = service4.get();

        RangeQueryIntermmediateResult rsRt1 = realtTime.get()[0];
        RangeQueryIntermmediateResult rsRt2 = realtTime.get()[1];
        RangeQueryIntermmediateResult rsRt3 = realtTime.get()[2];
        RangeQueryIntermmediateResult rsRt4 = realtTime.get()[3];
        executorService.shutdown();

        RangeQueryResult service1Rs = merge(rsBv1, rsRt1);
        RangeQueryResult service2Rs = merge(rsBv2, rsRt2);
        RangeQueryResult service3Rs = merge(rsBv3, rsRt3);
        RangeQueryResult service4Rs = merge(rsBv4, rsRt4);

        return convertToMap(service1Rs, service2Rs, service3Rs, service4Rs);
    }

    private BV_QueryInput createInput(int serviceId,FileSystem fs, DateTime start, DateTime end) throws SQLException, ClassNotFoundException {
        BV_QueryInput input = new BV_QueryInput();
        input.serviceId = serviceId;
        input.start = start;
        input.end = end;

        Class.forName("org.duckdb.DuckDBDriver");
        input.duckDbConn = DriverManager.getConnection("jdbc:duckdb:");

        input.fs = fs;
        return input;
    }

    private String convertTimestampToPattern(long timestamp) {
        DateTime dt = new DateTime(timestamp * 1000L);
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
        String dtStr = fmt.print(dt);
        return dtStr;
    }

    private RangeQueryResult merge(RangeQueryIntermmediateResult bv, RangeQueryIntermmediateResult rt) {
        if(bv == null && rt == null) return null;
        if(bv  == null) bv = new RangeQueryIntermmediateResult();
        if(rt == null) rt = new RangeQueryIntermmediateResult();

        RangeQueryResult result = new RangeQueryResult();

        long msgTotal = bv.recordCount + rt.recordCount;
        result.cpuUtilAvg = (bv.cpuUtilTotal + rt.cpuUtilTotal) / msgTotal;
        result.ramUtilAvg = (bv.ramUtilTotal + rt.ramUtilTotal) / msgTotal;
        result.diskUtilAvg = (bv.diskUtilTotal + rt.diskUtilTotal) / msgTotal;

        if(bv.cpuUtilMax > rt.cpuUtilMax) {
            result.cpuPeakTime = convertTimestampToPattern(bv.cpuPeakTime);
        } 
        else {
            result.cpuPeakTime = convertTimestampToPattern(rt.cpuPeakTime);
        }

        if(bv.ramUtilMax > rt.ramUtilMax) {
            result.ramPeakTime = convertTimestampToPattern(bv.ramPeakTime);
        } 
        else {
            result.ramPeakTime = convertTimestampToPattern(rt.ramPeakTime);
        }

        if(bv.diskUtilMax > rt.diskUtilMax) {
            result.diskPeakTime = convertTimestampToPattern(bv.diskPeakTime);
        } 
        else {
            result.diskPeakTime = convertTimestampToPattern(rt.diskPeakTime);
        }

        result.msgCount = bv.msgCount + rt.msgCount;
        return result;
    }

    private HashMap<String, String> convertToMap(RangeQueryResult... results){
        HashMap<String, String> map = new HashMap<>();
        StringBuilder sb = new StringBuilder();

        int count = 0;
        for(RangeQueryResult result: results) {
            count ++;
            sb.setLength(0);
            sb.append(result.cpuUtilAvg).append(" ");
            sb.append(result.cpuPeakTime).append(" ");
            sb.append(result.ramUtilAvg).append(" ");
            sb.append(result.ramPeakTime).append(" ");            
            sb.append(result.diskUtilAvg).append(" ");
            sb.append(result.diskPeakTime).append(" ");
            sb.append(result.msgCount);   
            
            map.put("service-" + count, sb.toString());
        }

        return map;
    }
}
