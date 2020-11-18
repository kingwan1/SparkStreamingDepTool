/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.Demo1Message;
import com.stream.calc.common.AnalysisMessageToBeanNode;

import scala.Tuple2;

/**
 * Demo1AnalysisBeanNode
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1AnalysisBeanNode extends AnalysisMessageToBeanNode<String, Demo1Message> {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo1AnalysisBeanNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /*
     * 拼接映射字段field
     * 
     */
    @Override
    public Tuple2<String[], Demo1Message> getFieldOrder(String t) {
        String[] tmpKeyValues = { "chgTag:0", "ignInfo:1", "appId:2", "userId:3", "machineId:4", "serialNum:5",
                "chgType:6", "cj:7", "accessId:8", "clkTime:9", "planId:10", "price:11", "chgTime:12", "chgPrice:13",
                "balance:14", "urate:15", "rrate:16", "srate:17", "orderRow:18", "masterId:19", "fyRuleId:20",
                "fyCouponId:21", "fyCouponBalance:22", "fyCouponRate:23", "notCashRate:24", "cashRuleId:25",
                "matchCode:26" };
        Demo1Message fcClickCsmMessage = new Demo1Message();
        return new Tuple2<String[], Demo1Message>(tmpKeyValues, fcClickCsmMessage);
    }

}
