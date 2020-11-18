/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.kafka.demo2;

import java.util.Date;
import java.util.Map;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.KafkaMapDuplicateCalcNode;
import com.stream.util.DateUtil;
import com.stream.util.TableUtil;

/**
 * @author wzs
 * @version 1.0
 * @title Demo2MapDuplicateCalcNode
 * @description 去重算子
 * @date 20/3/10
 */
public class Demo2MapDuplicateCalcNode extends KafkaMapDuplicateCalcNode {
    private static final long serialVersionUID = 1L;

    private final String TABLE_ROWKEY_DUP_PATTERN = "inc_d_demo2_%s_%s";

    // 初始化table
    private TableUtil table = null;

    /**
     * @param calcNodeParams
     */
    public Demo2MapDuplicateCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /**
     * 设置去重key
     * key 是通过固定值和 acct_id以及日期组成
     * @param map
     * @return
     */
    public String setKey(Map<String, String> map) {
        String dateStr = DateUtil.format(new Date(), DateUtil.YYYYMMDDHHMMSS);
        String key_val = String.format(TABLE_ROWKEY_DUP_PATTERN, map.get("acctId"), dateStr);
        System.out.println("key value : " + key_val);
        return key_val;
    }

}
