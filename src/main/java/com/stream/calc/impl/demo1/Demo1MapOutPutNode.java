/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.Demo1;
import com.stream.calc.common.SumOutPutTableCalcNode;
import com.stream.constant.ChargeConstant;

/**
 * Demo1MapOutPutNode 将数据与table合并计算，结果数据落库
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1MapOutPutNode extends SumOutPutTableCalcNode<Demo1, Demo1> {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo1MapOutPutNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /* 组合存入table的rowkey
     */
    @Override
    public String setKey(Demo1 demo1) {
        String rowKey = String.format(ChargeConstant.ZSYX_TABLE_ROWKEY_CALC_PATTERN, demo1.getClkTimeDay(),
                demo1.getClkTimeHour(), demo1.getAcctId(), demo1.getContractlineId());
        return rowKey;
    }

}
