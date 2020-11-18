/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.stream.bean.CalcNodeParams;

/**
 * calcNode 算子公共工具操作 map基础类
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class MapCalcNode<T, R> implements Function<JavaRDD<T>, JavaRDD<R>> {
    private static final Logger logger = Logger.getLogger(MapCalcNode.class);
    private static final long serialVersionUID = 1L;
    // 算子参数信息bean
    private CalcNodeParams calcNodeParams;
    // 算子ID,在一个job配置中唯一
    private String calcNodeID;

    /**
     * 构造，传入算子相关参数
     * 
     * @param calcNodeParams 传入算子相关信息
     */
    public MapCalcNode(CalcNodeParams calcNodeParams) {
        super();
        this.calcNodeParams = calcNodeParams;
    }

    public abstract R process(T v) throws Exception;

    /**
     * 初始化算子参数以及实例化db连接
     * 
     * @param rdd
     */
    private void initParams(JavaRDD<T> rdd) {
        this.calcNodeID = this.calcNodeParams.getCalcNodeID();
    }

    /**
     * 处理rdd算子功能操作
     * 
     * @param v
     * @return JavaRDD<R>
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public JavaRDD<R> call(JavaRDD<T> rdd) {
        // 初始化算子相关参数
        this.initParams(rdd);
        JavaRDD<T> tmpRdd = rdd;
        // 判断前置partition
        if (this.calcNodeParams.getPrePartition() > 0) {
            tmpRdd = rdd.repartition(this.calcNodeParams.getPrePartition());
        }
        JavaRDD<R> javaRdd = tmpRdd.map(new Function<T, R>() {
            private static final long serialVersionUID = 1L;

            @Override
            public R call(T v) throws Exception {
                logger.debug(String.format("run calcNodeId: %s", calcNodeID));
                return (R) process(v);
            }

        });
        JavaRDD<R> tempRdd = javaRdd;
        // 判断后置partition
        if (this.calcNodeParams.getPostPartition() > 0) {
            tempRdd = javaRdd.repartition(this.calcNodeParams.getPostPartition());
        }
        return tempRdd;
    }
}
