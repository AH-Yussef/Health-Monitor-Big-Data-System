package com.epokh.hdfs.Query;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RTV_RangeQuery implements Callable<RangeQueryIntermmediateResult[]>{
    private final String rangeSql = "SELECT serviceId, SUM(cpuUtil), MAX(cpuUtil), SUM(ramUtil), MAX(ramUtil), SUM(diskUtil), MAX(diskUtil), COUNT(*) FROM realtimeView WHERE timestamp BETWEEN %s AND %s GROUP BY serviceId";
    private final String peakTimeSql = "SELECT timestamp FROM realtimeView WHERE serviceId = %s AND ABS(%s - %s) < 0.00001 AND timestamp BETWEEN %s AND %s";

    private SparkSession spark;
    private long start;
    private long end;

    public RTV_RangeQuery(SparkSession spark, long start, long end) {
        this.spark = spark;
        this.start = start;
        this.end = end;
    }

    public RangeQueryIntermmediateResult[] execute() {
        Dataset<Row> df = spark.read().parquet("hdfs://hadoopmaster:9000/realtime/");
        df.createOrReplaceTempView("realtimeView");

        start = (start / 60) * 60;
        end = (end / 60) * 60;
        Dataset<Row> sqlRes = spark.sql(String.format(rangeSql, start, end));
        List<Row> rows = sqlRes.collectAsList();
      
        RangeQueryIntermmediateResult[] serviceResult = new RangeQueryIntermmediateResult[4];
        for(Row row: rows) {
            int serviceId = row.getInt(0);
            serviceResult[serviceId -1] = new RangeQueryIntermmediateResult();

            serviceResult[serviceId -1].cpuUtilTotal = row.getFloat(1);
            serviceResult[serviceId -1].cpuUtilMax = row.getFloat(2);
            serviceResult[serviceId -1].cpuPeakTime = spark.sql(String.format(peakTimeSql, serviceId, "cpuUtil", serviceResult[serviceId -1].cpuUtilMax, start, end)).collectAsList().get(0).getLong(0);

            serviceResult[serviceId -1].ramUtilTotal = row.getFloat(3);
            serviceResult[serviceId -1].ramUtilMax = row.getFloat(4);
            serviceResult[serviceId -1].ramPeakTime = spark.sql(String.format(peakTimeSql, serviceId, "cpuUtil", serviceResult[serviceId -1].ramUtilMax, start, end)).collectAsList().get(0).getLong(0);

            serviceResult[serviceId -1].diskUtilTotal = row.getFloat(5);
            serviceResult[serviceId -1].diskUtilMax = row.getFloat(6);
            serviceResult[serviceId -1].diskPeakTime = spark.sql(String.format(peakTimeSql, serviceId, "cpuUtil", serviceResult[serviceId -1].diskUtilMax, start, end)).collectAsList().get(0).getLong(0);

            serviceResult[serviceId -1].msgCount = row.getLong(7);
            serviceResult[serviceId -1].recordCount = row.getLong(7);
        }
        
        return serviceResult;
    }

    @Override
    public RangeQueryIntermmediateResult[] call() throws Exception {        
        return execute();
    }
}
