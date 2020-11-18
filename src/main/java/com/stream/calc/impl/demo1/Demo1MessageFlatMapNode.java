/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import java.util.Arrays;
import java.util.Iterator;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.FlatMapCalcNode;
import com.stream.constant.ChargeConstant;

/**
 * Demo1MessageFlatMapNode message pack 处理成lines
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1MessageFlatMapNode extends FlatMapCalcNode<String, String> {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo1MessageFlatMapNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /*
     * 将多行lines 转成 line
     * 
     */
    @Override
    public Iterator<String> process(String v) {
        // 避免出现null
        if (null == v) {
            v = "";
        }
        return Arrays.asList(v.split(ChargeConstant.PACK_LINE_SEPARATOR)).iterator();
    }

}
