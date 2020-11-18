package com.stream.constant;

/**
 * @author wzs
 * @version 1.0
 * @title KafkaOpTypeEnum
 * @description kafka操作类型枚举
 * @date 20/3/13
 */
public enum KafkaOpTypeEnum {

    // INSERT(0, "添加"), UPDATE(1, "更新"), DELETE(2, "删除");
    INSERT(0, "添加"), DELETE(1, "删除"), UPDATE(2, "更新");

    private KafkaOpTypeEnum(int code, String desc) {
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
