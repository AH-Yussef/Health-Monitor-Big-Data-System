package com.epokh.hdfs.RealtimeView;

import java.io.Serializable;

public class HealthMsg implements Serializable{
    private int serviceId;
    private long timestamp;
    private float cpuUtil;
    private float ramUtil;
    private float diskUtil;

    public int getServiceId() {
        return serviceId;
    }
    public float getDiskUtil() {
        return diskUtil;
    }
    public void setDiskUtil(float diskUtil) {
        this.diskUtil = diskUtil;
    }
    public float getRamUtil() {
        return ramUtil;
    }
    public void setRamUtil(float ramUtil) {
        this.ramUtil = ramUtil;
    }
    public float getCpuUtil() {
        return cpuUtil;
    }
    public void setCpuUtil(float cpuUtil) {
        this.cpuUtil = cpuUtil;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = (timestamp / 60) * 60;
    }
    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }
}
