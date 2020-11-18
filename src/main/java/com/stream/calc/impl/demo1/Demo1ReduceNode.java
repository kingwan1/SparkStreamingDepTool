/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.Demo1;
import com.stream.calc.common.SumReduceByKeyCalcNode;

/**
 * Demo1ReduceNode reduce求和计算，根据key
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1ReduceNode extends SumReduceByKeyCalcNode<Demo1, Demo1> {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo1ReduceNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /*
     * 拼接reducebykey key计算组合
     * 
     */
    @Override
    public Object setKey(Demo1 demo1) {
        String tmpKey = demo1.getClkTimeDay() + demo1.getClkTimeHour() + demo1.getAcctId()
                + demo1.getContractlineId();
        return tmpKey;
    }

}
