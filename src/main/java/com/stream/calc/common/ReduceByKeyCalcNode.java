/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.stream.bean.CalcNodeParams;

import scala.Tuple2;

/**
 * mapToPaircalcNode 算子公共工具操作 reduce操作工具类
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class ReduceByKeyCalcNode<T, R> implements Function<JavaRDD<T>, JavaRDD<R>> {
    private static final Logger logger = Logger.getLogger(ReduceByKeyCalcNode.class);
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
    public ReduceByKeyCalcNode(CalcNodeParams calcNodeParams) {
        super();
        this.calcNodeParams = calcNodeParams;
    }

    /**
     * 设置进行reduceByKey计算的key值
     * 
     * @param t
     * @return
     */
    public abstract Object setKey(T t);

    /**
     * process核心计算处理
     * 
     * @param v1 reduce计算对象v1
     * @param v2 reduce计算对象v2
     * @return
     */
    public abstract T process(T v1, T v2);

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
    @SuppressWarnings("unchecked")
    @Override
    public JavaRDD<R> call(JavaRDD<T> rdd) {
        // 初始化算子相关参数
        this.initParams(rdd);
        JavaRDD<T> tmpRdd = rdd;
        // 判断前置partition
        if (this.calcNodeParams.getPrePartition() > 0) {
            tmpRdd = rdd.repartition(this.calcNodeParams.getPrePartition());
        }
        // 将rdd转换成pairRdd
        JavaPairRDD<Object, T> pairRdd = tmpRdd.mapToPair(new PairFunction<T, Object, T>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Object, T> call(T t) throws Exception {
                // TODO Auto-generated method stub
                Object obj = setKey(t);
                return new Tuple2<Object, T>(obj, t);
            }
        });

        // pairRdd 聚合计算reduceByKey
        JavaPairRDD<Object, T> tmpPairRdd = pairRdd.reduceByKey(new Function2<T, T, T>() {
            private static final long serialVersionUID = 1L;

            @Override
            public T call(T v1, T v2) throws Exception {
                logger.debug(String.format("run calcNodeId: %s", calcNodeID));
                return process(v1, v2);
            }
        });

        // pairRdd转成rdd
        JavaRDD<T> javaRdd = tmpPairRdd.map(new Function<Tuple2<Object, T>, T>() {
            private static final long serialVersionUID = 1L;

            @Override
            public T call(Tuple2<Object, T> v1) throws Exception {
                // TODO Auto-generated method stub
                return v1._2();
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
