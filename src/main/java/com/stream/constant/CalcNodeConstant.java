/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.constant;

import java.io.Serializable;

/**
 * 算子中出现的固定参数
 * 
 * @author wzs
 * @date 2018-03-16
 */
public class CalcNodeConstant implements Serializable {
    private static final long serialVersionUID = 1L;
    // calcNode相关
    public static final String CALC_NODE_STATUS = "status";
    public static final String CALC_NODE_RUNNING = "running";
    public static final String CALC_NODE_FAILED = "failed";
    public static final String CALC_NODE_FINISHED = "finished";
    // rowkeys 分隔符
    public static final String CALC_NODE_ROWKEY_SEPARATOR = "\001";
    // rowKeys 在zookeeper存储key值
    public static final String CALC_NODE_ROKEYS = "rowKeys";
    // table 在去重column 列名
    public static final String DUP_TABLE_COLUMN_NUM = "num";
    // table family列簇cf1
    public static final String TABLE_CF_ONE = "cf1";

    // 剔除serialVersionUID
    public static final String BEAN_NON_FIELD_SERI = "serialVersionUID";
    // 剔除log字段
    public static final String BEAN_NON_FIELD_LOG = "logger";
    // 去重标记数字字符串 "1"
    public static final String DUPLICATE_NUM = "1";    
    // scv table name
    public static final String TABLE_NAME = "streaming.table.name";
    // scv table cf name
    public static final String TABLE_CF = "streaming.table.cf";
    

}
