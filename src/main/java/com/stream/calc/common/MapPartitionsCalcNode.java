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
 * 分区MAP处理算子基类
 *
 * @author wzs
 * @date 2018-09-10
 */
public abstract class MapPartitionsCalcNode<T, R> implements Function<JavaRDD<T>, JavaRDD<R>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(MapPartitionsCalcNode.class);

    // 算子参数信息bean
    private CalcNodeParams calcNodeParams;
    // 算子ID,在一个job配置中唯一
    private String calcNodeID;

    /**
     * 构造，传入算子相关参数
     * 
     * @param calcNodeParams 传入算子相关信息
     */
    public MapPartitionsCalcNode(CalcNodeParams calcNodeParams) {
        super();
        this.calcNodeParams = calcNodeParams;
    }

    /**
     * 获取算子参数信息
     * 
     * @return
     */
    public CalcNodeParams getCalcNodeParams() {
        return this.calcNodeParams;
    }

    public abstract Iterator<R> process(Iterator<T> v) throws Exception;

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
        JavaRDD<R> javaRdd = tmpRdd.mapPartitions(new FlatMapFunction<java.util.Iterator<T>, R>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<R> call(Iterator<T> arg0) throws Exception {
                LOGGER.debug(String.format("run calcNodeId: %s", calcNodeID));
                return process(arg0);
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
