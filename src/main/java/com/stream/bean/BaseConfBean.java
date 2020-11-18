/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.util.Map;

/**
 * 基本配置信息
 *
 * @author wzs
 * @date 2018-03-29
 */
public class BaseConfBean {

    // zk配置信息
    private Map<String, Object> zookeeper;
    // table配置信息
    private Map<String, Object> table;
    // hdfs配置信息
    private Map<String, Object> hdfs;
    
    /**
     * @return the zookeeper
     */
    public Map<String, Object> getZookeeper() {
        return zookeeper;
    }
    /**
     * @param zookeeper the zookeeper to set
     */
    public void setZookeeper(Map<String, Object> zookeeper) {
        this.zookeeper = zookeeper;
    }
    /**
     * @return the table
     */
    public Map<String, Object> getTable() {
        return table;
    }
    /**
     * @param table the table to set
     */
    public void setTable(Map<String, Object> table) {
        this.table = table;
    }
    /**
     * @return the hdfs
     */
    public Map<String, Object> getHdfs() {
        return hdfs;
    }
    /**
     * @param hdfs the hdfs to set
     */
    public void setHdfs(Map<String, Object> hdfs) {
        this.hdfs = hdfs;
    }
}
