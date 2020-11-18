/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.constant;

/**
 * Binlog操作类型枚举
 *
 * @author wzs
 * @date 2018-09-07
 */
public enum BinlogOpTypeEnum {

    ADD(0, "添加"), DELETE(1, "删除"), UPDATE(2, "更新");

    private BinlogOpTypeEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    private int code;
    private String desc;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
