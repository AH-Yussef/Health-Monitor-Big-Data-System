package com.epokh.hdfs;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Shared {
    static BlockingQueue<DatagramPacket> queue = new LinkedBlockingDeque<>();
    static List<Long> recvTime = new ArrayList<>();

    public static final String bv_mon_primary = "/batch_views_1";
    public static final String bv_year_primary = "/years_reports_1";

    public static final String bv_mon_secondary = "/batch_views_2";
    public static final String bv_year_secondary = "/years_reports_2";

    public static final String rt_primary = "/realtime";

    public static String bv_mon_out = bv_mon_primary;
    public static String bv_year_out = bv_year_primary;

    public static String bv_query_mon_sc = bv_mon_primary;
    public static String bv_query_year_sc = bv_year_primary;
}
