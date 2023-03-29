package com.epokh.hdfs.Query;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.concurrent.Callable;

import com.epokh.hdfs.Shared;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

public class BV_RangeQuery implements Callable<RangeQueryIntermmediateResult>{
    private int serviceId;
    private FileSystem fs;
    private Connection duckDbConn;
    DateTime start;
    DateTime end;

    // private final String minReportPath = "hdfs://hadoopmaster:9000/batch_views/service%d/%d-%d.parquet";
    // private final String monReportPath = "hdfs://hadoopmaster:9000/years_reports/reprot_service-%d.parquet";
    
    private final String minReportPath = "hdfs://hadoopmaster:9000"+Shared.bv_query_mon_sc+"/service%d/%d-%d.parquet";
    private final String monReportPath = "hdfs://hadoopmaster:9000"+Shared.bv_query_year_sc+"/report_service-%d.parquet";
    
    private final String minBufferPath = "/media/sf_shared/service_%d/%d-%d.parquet";
    private final String monBufferPath = "/media/sf_shared/report_service-%d.parquet";

    private final String singleMonthInfoSql = "SELECT SUM(cpuUtil), SUM(ramUtil), SUM(diskUtil), SUM(msgCount), MAX(cpuUtil), MAX(ramUtil), MAX(diskUtil), COUNT(*) FROM '%s' WHERE minOfMonth BETWEEN %d AND %d";

    private final String startMonthInfoSql = "SELECT SUM(cpuUtil), SUM(ramUtil), SUM(diskUtil), SUM(msgCount), MAX(cpuUtil), MAX(ramUtil), MAX(diskUtil), COUNT(*) FROM '%s' WHERE minOfMonth >= %d";

    private final String endMonthInfoSql = "SELECT SUM(cpuUtil), SUM(ramUtil), SUM(diskUtil), SUM(msgCount), MAX(cpuUtil), MAX(ramUtil), MAX(diskUtil), COUNT(*) FROM '%s' WHERE minOfMonth <= %d";

    private final String midMonthInfoSql = "SELECT SUM(totalCpuUtil), SUM(totalRamUtil), SUM(totalDiskUtil), SUM(msgCount), SUM(recordCount), MAX(maxCpuUtil), MAX(maxRamUtil), MAX(maxDiskUtil) FROM '%s' WHERE month BETWEEN %d AND %d AND year BETWEEN %d AND %d";
    
    private final String singlePeakTimeSql = "SELECT minOfMonth FROM '%s' WHERE ABS(%s - %s) < 0.00001 AND minOfMonth BETWEEN %d AND %d";

    private final String startPeakTimeSql = "SELECT minOfMonth FROM '%s' WHERE ABS(%s - %s) < 0.00001 AND minOfMonth >= %d";

    private final String endPeakTimeSql = "SELECT minOfMonth FROM '%s' WHERE ABS(%s - %s) < 0.00001 AND minOfMonth <= %d";

    private final String midPeakTimeSql = "SELECT month, year, %s FROM '%s' WHERE ABS(%s - %s) < 0.00001 AND month BETWEEN %d AND %d AND year BETWEEN %d AND %d";

    public BV_RangeQuery(BV_QueryInput input) {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            duckDbConn = input.duckDbConn;   
            
            // Configuration config = new Configuration();
            // config.set("f.defaultFS", "hdfs://hadoopmaster:9000/");  
            fs = input.fs;

            this.serviceId = input.serviceId;
            this.start = input.start;
            this.end = input.end;
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    private void cleanup() throws IllegalArgumentException, IOException {
        int startMonth = start.getMonthOfYear();
        int startYear = start.getYear();

        int endMonth = end.getMonthOfYear();
        int endYear = end.getYear();
        
        if(startYear == endYear && startMonth == endMonth) {
            String target =  String.format(minBufferPath, serviceId, startMonth, startYear);
            File file = new File(target); 
            file.delete();
        }
        else if(startYear == endYear && (startMonth+1 %12) == endMonth) {
            String target =  String.format(minBufferPath, serviceId, startMonth, startYear);
            File file = new File(target); 
            file.delete();
            
            target =  String.format(minBufferPath, serviceId, endMonth, endYear);
            file = new File(target); 
            file.delete();
        }
        else {
            String target =  String.format(minBufferPath, serviceId, startMonth, startYear);
            File file = new File(target); 
            file.delete();
            
            target =  String.format(minBufferPath, serviceId, endMonth, endYear);
            file = new File(target); 
            file.delete();
            
            target =  String.format(monBufferPath, serviceId);
            file = new File(target); 
            file.delete();
        }
    }

    private void setup() throws IllegalArgumentException, IOException {
        int startMonth = start.getMonthOfYear();
        int startYear = start.getYear();

        int endMonth = end.getMonthOfYear();
        int endYear = end.getYear();

        String dest = "/media/sf_shared/service_"+serviceId;
        
        if(startYear == endYear && startMonth == endMonth) {
            String source =  String.format(minReportPath, serviceId, startMonth, startYear);
            fs.copyToLocalFile(false, new Path(source), new Path(dest));
        }
        else if(startYear == endYear && (startMonth+1 %12) == endMonth) {
            String source =  String.format(minReportPath, serviceId, startMonth, startYear);
            fs.copyToLocalFile(false, new Path(source), new Path(dest));
            
            source =  String.format(minReportPath, serviceId, endMonth, endYear);
            fs.copyToLocalFile(false, new Path(source), new Path(dest));
        }
        else {
            String source =  String.format(minReportPath, serviceId, startMonth, startYear);
            fs.copyToLocalFile(false, new Path(source), new Path(dest));
            
            source =  String.format(minReportPath, serviceId, endMonth, endYear);
            fs.copyToLocalFile(false, new Path(source), new Path(dest));
            
            source =  String.format(monReportPath, serviceId);
            fs.copyToLocalFile(false, new Path(source), new Path("/media/sf_shared"));
        }
    }

    private RangeQueryIntermmediateResult execute() throws SQLException, IllegalArgumentException, IOException {
        int startMonth = start.getMonthOfYear();
        int startYear = start.getYear();

        int endMonth = end.getMonthOfYear();
        int endYear = end.getYear();

        RangeQueryIntermmediateResult rangeQueryResult;
        
        if(startYear == endYear && startMonth == endMonth) {
            setup();
            RangeQueryIntermmediateResult res = getSingleMonthInfo();
            rangeQueryResult = combineIntermmediateResults(res);
            cleanup();
        }
        else if(startYear == endYear && (startMonth+1 %12) == endMonth) {
            setup();
            RangeQueryIntermmediateResult startRes = getMonthInfo(start, startMonthInfoSql, startPeakTimeSql);
            RangeQueryIntermmediateResult endRes = getMonthInfo(end, endMonthInfoSql, endPeakTimeSql);
            rangeQueryResult = combineIntermmediateResults(startRes, endRes);
            cleanup();
        }
        else {
            setup();
            RangeQueryIntermmediateResult startRes = getMonthInfo(start, startMonthInfoSql, startPeakTimeSql);
            RangeQueryIntermmediateResult endRes = getMonthInfo(end, endMonthInfoSql, endPeakTimeSql);
            RangeQueryIntermmediateResult midRes = getMidMonthsInfo();
            rangeQueryResult = combineIntermmediateResults(startRes, midRes, endRes);
            cleanup();
        }

        return rangeQueryResult;
    }

    private RangeQueryIntermmediateResult getSingleMonthInfo() throws SQLException {
        int year = start.getYear();
        int month = start.getMonthOfYear();
        int startMinOfMonth = (start.getDayOfMonth() - 1)*24*60 + start.getMinuteOfDay();
        int endMinOfMonth = (end.getDayOfMonth() - 1)*24*60 + end.getMinuteOfDay();

        String target = String.format(minBufferPath, serviceId, month, year);
        
        Statement stmt = duckDbConn.createStatement();
        ResultSet rs1 = stmt.executeQuery(String.format(singleMonthInfoSql, target, startMinOfMonth, endMinOfMonth));

        RangeQueryIntermmediateResult result = new RangeQueryIntermmediateResult();

        if(!rs1.next()) return null;
        result.cpuUtilTotal = rs1.getFloat(1);
        result.ramUtilTotal = rs1.getFloat(2);
        result.diskUtilTotal = rs1.getFloat(3);
        result.msgCount = rs1.getLong(4);
        result.cpuUtilMax = rs1.getFloat(5);
        result.ramUtilMax = rs1.getFloat(6);
        result.diskUtilMax = rs1.getFloat(7);
        result.recordCount = rs1.getLong(8);
        
        ResultSet rs2 = stmt.executeQuery(String.format(singlePeakTimeSql, target, "cpuUtil", result.cpuUtilMax, startMinOfMonth, endMinOfMonth));
        rs2.next();
        result.cpuPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        rs2 = stmt.executeQuery(String.format(singlePeakTimeSql, target, "ramUtil", result.ramUtilMax, startMinOfMonth, endMinOfMonth));
        rs2.next();
        result.ramPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        rs2 = stmt.executeQuery(String.format(singlePeakTimeSql, target, "diskUtil", result.diskUtilMax, startMinOfMonth, endMinOfMonth));
        rs2.next();
        result.diskPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        return result;
    }

    private RangeQueryIntermmediateResult getMonthInfo(DateTime dt, String basicInfoSql, String peakSql) throws SQLException {
        int year = dt.getYear();
        int month = dt.getMonthOfYear();
        int minOfMonth = (dt.getDayOfMonth() - 1)*24*60 + dt.getMinuteOfDay();

        String target = String.format(minBufferPath, serviceId, month, year);
        
        Statement stmt = duckDbConn.createStatement();
        ResultSet rs1 = stmt.executeQuery(String.format(basicInfoSql, target, minOfMonth));

        RangeQueryIntermmediateResult result = new RangeQueryIntermmediateResult();

        if(!rs1.next()) return null;
        result.cpuUtilTotal = rs1.getFloat(1);
        result.ramUtilTotal = rs1.getFloat(2);
        result.diskUtilTotal = rs1.getFloat(3);
        result.msgCount = rs1.getLong(4);
        result.cpuUtilMax = rs1.getFloat(5);
        result.ramUtilMax = rs1.getFloat(6);
        result.diskUtilMax = rs1.getFloat(7);
        result.recordCount = rs1.getInt(8);

        ResultSet rs2 = stmt.executeQuery(String.format(peakSql, target, "cpuUtil", result.cpuUtilMax, minOfMonth));
        rs2.next();
        result.cpuPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        rs2 = stmt.executeQuery(String.format(peakSql, target, "ramUtil", result.ramUtilMax, minOfMonth));
        rs2.next();
        result.ramPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        rs2 = stmt.executeQuery(String.format(peakSql, target, "diskUtil", result.diskUtilMax, minOfMonth));
        rs2.next();
        result.diskPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(1));

        return result;
    }

    private RangeQueryIntermmediateResult getMidMonthsInfo() throws SQLException {
        int startYear = start.getYear();
        int startMonth = start.getMonthOfYear();
        int endYear = start.getYear();
        int endMonth = start.getMonthOfYear();

        String target = String.format(monBufferPath, serviceId);
        
        Statement stmt = duckDbConn.createStatement();
        ResultSet rs1 = stmt.executeQuery(String.format(midMonthInfoSql, target, startMonth, endMonth, startYear,  endYear));

        RangeQueryIntermmediateResult result = new RangeQueryIntermmediateResult();

        if(!rs1.next()) return null;
        result.cpuUtilTotal = rs1.getFloat(1);
        result.ramUtilTotal = rs1.getFloat(2);
        result.diskUtilTotal = rs1.getFloat(3);
        result.msgCount = rs1.getLong(4);
        result.recordCount = rs1.getLong(5);
        result.cpuUtilMax = rs1.getFloat(6);
        result.ramUtilMax = rs1.getFloat(7);
        result.diskUtilMax = rs1.getFloat(8);
        
        ResultSet rs2 = stmt.executeQuery(String.format(midPeakTimeSql, "cpuPeakTime",target, "maxCpuUtil", result.cpuUtilMax, startMonth, endMonth, startYear, endYear));
        rs2.next();
        int month = rs2.getInt(1);
        int year = rs2.getInt(2);
        result.cpuPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(3));

        rs2 = stmt.executeQuery(String.format(midPeakTimeSql,"ramPeakTime", target, "maxRamUtil", result.ramUtilMax, startMonth, endMonth, startYear, endYear));
        rs2.next();
        month = rs2.getInt(1);
        year = rs2.getInt(2);
        result.cpuPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(3));

        rs2 = stmt.executeQuery(String.format(midPeakTimeSql, "diskPeakTime", target, "maxDiskUtil", result.diskUtilMax, startMonth, endMonth, startYear, endYear));
        rs2.next();       
        month = rs2.getInt(1);
        year = rs2.getInt(2);
        result.cpuPeakTime = convertTimeToTimestamp(year, month, rs2.getInt(3));

        return result;
    }

    private RangeQueryIntermmediateResult combineIntermmediateResults(RangeQueryIntermmediateResult... results) {
        float cpuUtilTotal = 0;
        float cpuUtilMax = 0;
        long cpuPeakTime = 0;

        float ramUtilTotal = 0;
        float ramUtilMax = 0;
        long ramPeakTime = 0;

        float diskUtilTotal = 0;
        float diskUtilMax = 0;
        long diskPeakTime = 0;

        int recordCount = 0;
        long msgCount = 0;

        int emptyCount = 0;

        for(RangeQueryIntermmediateResult result: results) {
            if(result == null) {
                emptyCount ++; 
                continue;
            }

            cpuUtilTotal += result.cpuUtilTotal;
            ramUtilTotal += result.ramUtilTotal;
            diskUtilTotal += result.diskUtilTotal;

            if (result.cpuUtilMax > cpuUtilMax) {
                cpuUtilMax = result.cpuUtilMax;
                cpuPeakTime = result.cpuPeakTime;
            }
            if (result.ramUtilMax > ramUtilMax) {
                ramUtilMax = result.ramUtilMax;
                ramPeakTime = result.ramPeakTime;
            }
            if (result.diskUtilMax > diskUtilMax) {
                diskUtilMax = result.diskUtilMax;
                diskPeakTime = result.diskPeakTime;
            }
            
            msgCount += result.msgCount;
            recordCount += result.recordCount;
        } 

        if(emptyCount == results.length) return null;

        RangeQueryIntermmediateResult finalResult = new RangeQueryIntermmediateResult();

        finalResult.cpuUtilTotal = cpuUtilTotal;
        finalResult.cpuUtilMax = cpuUtilMax;
        finalResult.cpuPeakTime = cpuPeakTime;

        finalResult.ramUtilTotal = ramUtilTotal;
        finalResult.ramUtilMax = ramUtilMax;
        finalResult.ramPeakTime = ramPeakTime;

        finalResult.diskUtilTotal = diskUtilTotal;
        finalResult.diskUtilMax = diskUtilMax;
        finalResult.diskPeakTime = diskPeakTime;
        
        finalResult.msgCount = msgCount;
        finalResult.recordCount = recordCount;

        return finalResult;
    }

    private long convertTimeToTimestamp(int year, int month, int minOfMonth) {
        int day = (minOfMonth / 1440) +1;
        int minOfDay = minOfMonth % 1440;
        int hour = minOfDay / 60;
        int minute = minOfDay % 60;

        System.out.println("\n\n\n\n\n"+year + " " + month + " " + day +" "+ hour + " " + minute);

        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month-1);
        c.set(Calendar.DAY_OF_MONTH, day);
        c.set(Calendar.HOUR, hour);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        System.out.println(c.getTimeInMillis() / 1000L);
    
        return c.getTimeInMillis() / 1000L;
    }

    @Override
    public RangeQueryIntermmediateResult call() throws Exception {
        return execute();
    }
}
