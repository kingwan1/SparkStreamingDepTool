package com.stream.constant;

/**
 * Created by wzs on 2020/3/8.
 */
public enum DataSourceType {
    BigPipe("bigpipe", "bigpipe"),
    Kafka("kafka", "kafka");

    private String code;
    private String name;

    private DataSourceType(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return this.code;
    }

    public String getName() {
        return this.name;
    }
}
