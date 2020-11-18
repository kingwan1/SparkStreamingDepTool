/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.stream.bean.CalcNodeParams;

/**
 * calcNode 算子公共工具操作 过滤算子基础类
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class FilterCalcNode<T, R> implements Function<JavaRDD<T>, JavaRDD<R>> {
    private static final Logger logger = Logger.getLogger(FilterCalcNode.class);
    private static final long serialVersionUID = 1L;
    // 算子参数信息bean
    private CalcNodeParams calcNodeParams;
    // 算子ID,在一个job配置中唯一
    private String calcNodeID;

    /**
     * @return the calcNodeID
     */
    public String getCalcNodeID() {
        return calcNodeID;
    }

    /**
     * @param calcNodeID the calcNodeID to set
     */
    public void setCalcNodeID(String calcNodeID) {
        this.calcNodeID = calcNodeID;
    }

    /**
     * 构造，传入算子相关参数
     * 
     * @param calcNodeParams 传入算子相关信息
     */
    public FilterCalcNode(CalcNodeParams calcNodeParams) {
        super();
        this.calcNodeParams = calcNodeParams;
    }

    public abstract Boolean process(T v) throws Exception;

    /**
     * 初始化算子参数以及实例化bd连接
     * 
     * @param rdd
     */
    private void initParams(JavaRDD<T> rdd) {
        this.calcNodeID = this.calcNodeParams.getCalcNodeID();
    }

    /**
     * 处理rdd算子功能操作
     * 
     * @param v1
     * @return JavaRDD<R>
     * @throws Exception
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public JavaRDD<R> call(JavaRDD<T> rdd) throws Exception {
        // 初始化算子相关参数
        this.initParams(rdd);
        JavaRDD<T> tmpRdd = rdd;
        // 判断前置partition
        if (this.calcNodeParams.getPrePartition() > 0) {
            tmpRdd = rdd.repartition(this.calcNodeParams.getPrePartition());
        }
        JavaRDD<T> javaRdd = tmpRdd.filter(new Function<T, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(T v) throws Exception {
                logger.debug(String.format("run calcNodeId: %s", calcNodeID));
                return process(v);
            }

        });

        JavaRDD<T> tempRdd = javaRdd;
        // 判断后置partition
        if (this.calcNodeParams.getPostPartition() > 0) {
            tempRdd = javaRdd.repartition(this.calcNodeParams.getPostPartition());
        }
        return (JavaRDD<R>) tempRdd;
    }
}
