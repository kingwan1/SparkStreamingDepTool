/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.stream.bean.CalcNodeParams;

/**
 * calcNode 算子工具基础类 flatMap函数打平操作
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class FlatMapCalcNode<T, R> implements Function<JavaRDD<T>, JavaRDD<R>> {
    private static final Logger logger = Logger.getLogger(FlatMapCalcNode.class);
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
    public FlatMapCalcNode(CalcNodeParams calcNodeParams) {
        super();
        this.calcNodeParams = calcNodeParams;
    }

    public abstract Iterator<R> process(T v);

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
        JavaRDD<R> javaRdd = tmpRdd.flatMap(new FlatMapFunction<T, R>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<R> call(T t) throws Exception {
                logger.debug(String.format("run calcNodeId: %s", calcNodeID));
                return process(t);
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
