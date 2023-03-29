package com.epokh.hdfs.Query;

import java.sql.Connection;

import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;

public class BV_QueryInput {
    public int serviceId;
    public DateTime start;
    public DateTime end;
    public FileSystem fs;
    public Connection duckDbConn;
}
