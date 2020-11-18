/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.MapCalcNode;
import com.stream.constant.ChargeConstant;
import com.stream.util.BeanUtil;

import scala.Tuple2;

/**
 * 日志解析处理成相应的bean
 * 
 * @author wzs
 * @date 2018-03-27
 */
public abstract class AnalysisMessageToBeanNode<T, R> extends MapCalcNode<T, R> {
    private static final Logger logger = Logger.getLogger(AnalysisMessageToBeanNode.class);
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public AnalysisMessageToBeanNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    /**
     * 设置 message消息解析后对应bean字段顺序 格式：{"crc:0", "version:1", "route:2"}
     * 
     * @param t 传入的message
     * @return Tuple2<String[], R> 字符串数据组是组合的字段顺序数据，R是映射的Bean实例
     */
    public abstract Tuple2<String[], R> getFieldOrder(T t);

    /**
     * 
     * @param v1 传入的message
     * @return 解析后的bean实例
     * @see com.stream.calc.common.MapCalcNode#process(java.lang.Object)
     */
    @Override
    public R process(T v1) {
        return this.analysisMessage(v1);
    }

    /**
     * 解析message实现
     * 
     * @param v1
     * @return
     */
    @SuppressWarnings("unchecked")
    public R analysisMessage(T v1) {
        Object tmpObject = null;
        try {
            Tuple2<String[], R> tmpTuple = this.getFieldOrder(v1);
            String[] fieldNames = tmpTuple._1();
            // 区分是否指定 解析映射Bean 的field字段顺序 , 如果有字段顺序，根据字段顺序判断，否则按照bean的字段顺序
            tmpObject = BeanUtil.string2Bean((String) v1, ChargeConstant.PACK_FIELD_SEPARATOR, tmpTuple._2(),
                    fieldNames);

        } catch (Exception e) {
            logger.error(String.format("%s exception in the calculation, and the exception information is: %s",
                    this.getClass().getName(), e.getMessage()), e);
        }
        return (R) tmpObject;
    }

}
