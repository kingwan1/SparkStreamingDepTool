/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeParams;

/**
 * 简单打印算子
 *
 * @author wzs
 * @date 2018-09-07
 */
public class SimplePrintMapCalcNode extends MapCalcNode<String, String> {
    private static final Logger LOGGER = Logger.getLogger(SimplePrintMapCalcNode.class);
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public SimplePrintMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    @Override
    public String process(String v1) {
        // TODO Auto-generated method stub
        // System.out.println(v1);
        LOGGER.info(v1);
        return v1;
    }
}
