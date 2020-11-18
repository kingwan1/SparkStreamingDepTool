/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.DuplicateFilterBean;
import com.stream.constant.CalcNodeConstant;
import com.stream.constant.EnvConfConstant;
import com.stream.util.BeanUtil;
import com.stream.util.DateUtil;
import com.stream.util.StringUtil;
import com.stream.util.TableUtil;

import table.java.api.Cell;

/**
 * 去重算子,分partition写入table
 *
 * @author wzs
 * @date 2018-12-29
 */
public abstract class DuplicateMapFilterCalcNode<T> implements Function<JavaRDD<T>, JavaRDD<T>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(DuplicateMapFilterCalcNode.class);
    // 算子参数信息bean
    private CalcNodeParams calcNodeParams;
    // 算子ID,在一个job配置中唯一
    private String calcNodeID;

    /**
     * 构造，传入算子相关参数
     * 
     * @param calcNodeParams 传入算子相关信息
     */
    public DuplicateMapFilterCalcNode(CalcNodeParams calcNodeParams) {
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

    /**
     * 初始化算子参数以及实例化db连接
     * 
     * @param rdd
     */
    private void initParams(JavaRDD<T> rdd) {
        this.calcNodeID = this.calcNodeParams.getCalcNodeID();
    }

    /**
     * 去重设置计算的key值组合
     * 
     * @param t 要进行去重的实例对象
     * @return String 去重的key
     */
    public abstract String setKey(T t);

    /**
     * 处理rdd算子功能操作
     * 
     * @param v
     * @return JavaRDD<R>
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    @Override
    public JavaRDD<T> call(JavaRDD<T> rdd) throws Exception {
        // 初始化算子相关参数
        this.initParams(rdd);
        JavaRDD<T> tmpRdd = rdd;
        // 判断前置partition
        if (this.calcNodeParams.getPrePartition() > 0) {
            tmpRdd = rdd.repartition(this.calcNodeParams.getPrePartition());
        }
        JavaRDD<T> javaRdd = tmpRdd.mapPartitions(new FlatMapFunction<java.util.Iterator<T>, DuplicateFilterBean<T>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<DuplicateFilterBean<T>> call(Iterator<T> arg0) throws Exception {
                LOGGER.debug(String.format("run calcNodeId-mapPartition: %s", calcNodeID));
                LOGGER.info(String.format("Start to Connect to Table: %s",
                        DateUtil.format(new Date(), DateUtil.YYYYMMDDHHMMSS)));
                TableUtil table = null;
                Map<String, String> custParam = getCalcNodeParams().getCustParams();
                if (custParam != null) {
                    String tableName = custParam.get(EnvConfConstant.TABLE_KEY_TABLE_NAME);
                    if (!StringUtil.isNullStr(tableName)) {
                        table = new TableUtil(tableName);
                    } else {
                        table = new TableUtil();
                    }
                } else {
                    table = new TableUtil();
                }
                LOGGER.info(String.format("Connected to Table, Start to put data: %s",
                        DateUtil.format(new Date(), DateUtil.YYYYMMDDHHMMSS)));
                List<DuplicateFilterBean<T>> resList = new ArrayList<DuplicateFilterBean<T>>();
                while (arg0.hasNext()) {
                    T v = arg0.next();
                    String rowKey = setKey(v);
                    Cell cellNum = table.selectCell(CalcNodeConstant.TABLE_CF_ONE,
                            CalcNodeConstant.DUP_TABLE_COLUMN_NUM, rowKey);
                    Map<String, Object> mapColumn = new HashMap<String, Object>();
                    DuplicateFilterBean<T> filterBean = new DuplicateFilterBean<T>();
                    filterBean.setObj(v);
                    if (null != cellNum) {
                        // 重复出现加1
                        int tempCellClk = Integer.parseInt(new String(cellNum.getValue())) + 1;
                        mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, String.valueOf(tempCellClk));
                        filterBean.setValid(Boolean.FALSE);
                        resList.add(filterBean);
                    } else {
                        // 首次出现写在table
                        mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, CalcNodeConstant.DUPLICATE_NUM);
                        Field[] fields = v.getClass().getDeclaredFields();
                        // 将bean写入table
                        for (Field field : fields) {
                            Object value = BeanUtil.invokGetMethod(v, field.getName());
                            // 剔除非法field
                            if (CalcNodeConstant.BEAN_NON_FIELD_SERI.equals(field.getName())
                                    || CalcNodeConstant.BEAN_NON_FIELD_LOG.equals(field.getName())) {
                                continue;
                            }
                            if (value instanceof String) {
                                mapColumn.put(field.getName(), (String) value);
                            } else {
                                mapColumn.put(field.getName(), String.valueOf(value));
                            }
                        }
                        filterBean.setValid(Boolean.TRUE);
                        resList.add(filterBean);
                    }
                    Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
                    mapFamily.put(CalcNodeConstant.TABLE_CF_ONE, mapColumn);
                    table.writeColumnsByRow(rowKey, mapFamily);
                }

                table.closeTable();
                LOGGER.info(String.format("Close Table: %s", DateUtil.format(new Date(), DateUtil.YYYYMMDDHHMMSS)));
                return resList.iterator();
            }
        }).filter(new Function<DuplicateFilterBean<T>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(DuplicateFilterBean<T> v) throws Exception {
                LOGGER.debug(String.format("run calcNodeId-filter: %s", calcNodeID));
                return v.isValid();
            }
        }).map(new Function<DuplicateFilterBean<T>, T>() {

            private static final long serialVersionUID = 1L;

            @Override
            public T call(DuplicateFilterBean<T> arg0) throws Exception {
                return arg0.getObj();
            }

        });
        JavaRDD<T> tempRdd = javaRdd;
        // 判断后置partition
        if (this.calcNodeParams.getPostPartition() > 0) {
            tempRdd = javaRdd.repartition(this.calcNodeParams.getPostPartition());
        }
        return tempRdd;
    }
}
