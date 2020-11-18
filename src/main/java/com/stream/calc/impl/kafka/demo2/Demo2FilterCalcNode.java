package com.stream.calc.impl.kafka.demo2;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.KafkaFilterCalcNode;

/**
 * @author wzs
 * @version 1.0
 * @title Demo2FilterCalcNode
 * @description 过滤算子
 * @date 20/3/10
 */
public class Demo2FilterCalcNode extends KafkaFilterCalcNode {
    private static final long serialVersionUID = 1L;

    public static final int FIELD_LENGTH = 3;

    /**
     * @param calcNodeParams
     */
    public Demo2FilterCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    @Override
    public int setFieldLen() {
        System.out.println(" setFieldLen2.0: son's Field Length : " + FIELD_LENGTH + "  ");
        return FIELD_LENGTH;
    }
}
