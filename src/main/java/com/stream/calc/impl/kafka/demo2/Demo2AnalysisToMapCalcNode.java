package com.stream.calc.impl.kafka.demo2;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.KafkaAnalysisToMapCalcNode;

/**
 * @author wzs
 * @version 1.0
 * @title Demo2AnalysisToMapCalcNode
 * @description 分析算子
 * @date 20/3/10
 */
public class Demo2AnalysisToMapCalcNode extends KafkaAnalysisToMapCalcNode {

    private static final long serialVersionUID = 1L;

    // 算子调用时候的传入参数bean
    private CalcNodeParams calcNodeParams;

    /**
     * @param calcNodeParams 构造器使用定义参数
     */
    public Demo2AnalysisToMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        this.calcNodeParams = calcNodeParams;

    }
}
