package com.stream.calc.impl.kafka.demo2;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.KafkaFlatMapCalcNode;

/**
 * @author wzs
 * @version 1.0
 * @title Demo2FlatMapCalcNode
 * @description 拆分算子
 * @date 20/3/10
 */
public class Demo2FlatMapCalcNode extends KafkaFlatMapCalcNode {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo2FlatMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

}
