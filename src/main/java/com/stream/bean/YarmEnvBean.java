/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.bean;

import java.util.List;
import java.util.Map;

/**
 *
 * Description: 环境变量yarm参数
 *
 * @author wzs
 * @date 2018-03-08
 * @Version 1.0.0
 */
public class YarmEnvBean {
    private String appName;
    private long duration;
    private String master;
    private Map<String, Object> params;
    private List<Map<String, Object>> bigPipe;
    private List<Map<String, Object>> calcTemplate;
    private List<Map<String, Object>> kafka;
    private String dataSourceType = "bigpipe";

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public List<Map<String, Object>> getBigPipe() {
        return bigPipe;
    }

    public void setBigPipe(List<Map<String, Object>> bigPipe) {
        this.bigPipe = bigPipe;
    }

    public List<Map<String, Object>> getCalcTemplate() {
        return calcTemplate;
    }

    public void setCalcTemplate(List<Map<String, Object>> calcTemplate) {
        this.calcTemplate = calcTemplate;
    }

    @Override
    public String toString() {
        return "yarmEvn {" + "appName=" + appName + ", duration='" + duration + '\'' + ", params=" + params
                + ", calcTemplate=" + calcTemplate + '}';
    }

    public List<Map<String, Object>> getKafka() {
        return kafka;
    }

    public void setKafka(List<Map<String, Object>> kafka) {
        this.kafka = kafka;
    }

    public String getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(String dataSourceType) {
        this.dataSourceType = dataSourceType;
    }
}
