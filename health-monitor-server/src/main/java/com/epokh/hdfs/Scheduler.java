package com.epokh.hdfs;

import com.epokh.hdfs.BatchView.BatchViewsHandler;

public class Scheduler implements Runnable{
    String mon_out[] = {Shared.bv_mon_primary, Shared.bv_mon_secondary};
    String year_out[] = {Shared.bv_year_primary, Shared.bv_year_secondary};
    
    @Override
    public void run() {
        int i = 0;
        while(true) {
            try {
                Thread.sleep(3_600_000L);
                Shared.bv_mon_out = mon_out[i];
                Shared.bv_year_out = year_out[i];
                i++;
                i %= 2;
                BatchViewsHandler handler = new BatchViewsHandler();
                handler.generate();

                Shared.bv_query_mon_sc = mon_out[(i+1)%2];
                Shared.bv_query_year_sc = year_out[(i+1)%2];
            } catch (Exception e) {
                e.printStackTrace();
            }        
        }   
    }
}
