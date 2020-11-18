/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stream.bean.CalcNodeParams;
import com.stream.constant.CalcNodeConstant;
import com.stream.util.BeanUtil;
import com.stream.util.TableUtil;

import table.java.api.Cell;

/**
 * calcNode 算子工具操作 sum与table计算结果重新写入table
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class SumOutPutTableCalcNode<T, R> extends MapCalcNode<T, R> {
    private static final long serialVersionUID = 1L;
    // table初始化
    private TableUtil table = null;

    /**
     * @param calcNodeParams
     */
    public SumOutPutTableCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /**
     * 设置与table 值sum计算的业务rowkey
     * 
     * @param t 处理的Bean对象
     * @return
     */
    public abstract String setKey(T t);

    /**
     * 将列簇cell 转成 value值 long类型
     * 
     * @param familyResult 获取列簇值
     * @param key 获取value对应的key
     * @return
     */
    public long getCell2Long(Map<String, Map<String, Cell>> familyResult, String key) {
        long value = 0;
        if (null != familyResult) {
            Map<String, Cell> columnResult = familyResult.get(CalcNodeConstant.TABLE_CF_ONE);
            Cell cell = columnResult.get(key);
            if (null != cell) {
                // cell 存在已有值，需要更新数据
                value = Long.parseLong(new String(cell.getValue()));

            }

        }
        return value;
    }

    /**
     * 将列簇cell 转成 value值 int类型
     * 
     * @param familyResult 获取列簇值
     * @param key 获取value对应的key
     * @return
     */
    public long getCell2Int(Map<String, Map<String, Cell>> familyResult, String key) {
        int value = 0;
        if (null != familyResult) {
            Map<String, Cell> columnResult = familyResult.get(CalcNodeConstant.TABLE_CF_ONE);
            Cell cell = columnResult.get(key);
            if (null != cell) {
                // cell 存在已有值，需要更新数据
                value = Integer.parseInt(new String(cell.getValue()));

            }

        }
        return value;
    }

    /**
     * 将列簇cell 转成 value值 double类型
     * 
     * @param familyResult 获取列簇值
     * @param key 获取value对应的key
     * @return
     */
    public double getCell2Double(Map<String, Map<String, Cell>> familyResult, String key) {
        double value = 0;
        if (null != familyResult) {
            Map<String, Cell> columnResult = familyResult.get(CalcNodeConstant.TABLE_CF_ONE);
            Cell cell = columnResult.get(key);
            if (null != cell) {
                // cell 存在已有值，需要更新数据
                value = Double.parseDouble(new String(cell.getValue()));

            }

        }
        return value;
    }

    /**
     * 将列簇cell 转成 value值 float类型
     * 
     * @param familyResult 获取列簇值
     * @param key 获取value对应的key
     * @return
     */
    public float getCell2Float(Map<String, Map<String, Cell>> familyResult, String key) {
        float value = 0;
        if (null != familyResult) {
            Map<String, Cell> columnResult = familyResult.get(CalcNodeConstant.TABLE_CF_ONE);
            Cell cell = columnResult.get(key);
            if (null != cell) {
                // cell 存在已有值，需要更新数据
                value = Float.parseFloat(new String(cell.getValue()));

            }

        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public R process(T v2) throws Exception {
        String rowKey = this.setKey(v2);
        Map<String, Object> mapColumn = new HashMap<String, Object>();
        if (null == table) {
            table = new TableUtil();
        }
        Map<String, List<String>> columns = new HashMap<String, List<String>>();
        columns.put(CalcNodeConstant.TABLE_CF_ONE, null);
        // 查出rowkey对应的values
        Map<String, Map<String, Cell>> familyResult = table.selectColumnsByRow(columns, rowKey);

        Field[] fields = v2.getClass().getDeclaredFields();
        // 将bean写入table
        for (Field field : fields) {
            Object value = BeanUtil.invokGetMethod(v2, field.getName());
            // 剔除非法field
            if (CalcNodeConstant.BEAN_NON_FIELD_SERI.equals(field.getName())) {
                continue;
            }
            switch (field.getType().toString()) {
                case "long":
                    long cellLong = (long) value + getCell2Long(familyResult, field.getName());
                    mapColumn.put(field.getName(), String.valueOf(cellLong));
                    BeanUtil.invokSetMethod(v2, field.getName(), cellLong, field.getType());
                    break;
                case "double":
                    double cellDouble = (double) value + getCell2Double(familyResult, field.getName());
                    mapColumn.put(field.getName(), String.valueOf(cellDouble));
                    BeanUtil.invokSetMethod(v2, field.getName(), cellDouble, field.getType());
                    break;
                case "float":
                    float cellFloat = (float) value + getCell2Float(familyResult, field.getName());
                    mapColumn.put(field.getName(), String.valueOf(cellFloat));
                    BeanUtil.invokSetMethod(v2, field.getName(), cellFloat, field.getType());
                    break;
                case "int":
                    double cellInt = (int) value + getCell2Int(familyResult, field.getName());
                    mapColumn.put(field.getName(), String.valueOf(cellInt));
                    BeanUtil.invokSetMethod(v2, field.getName(), cellInt, field.getType());
                    break;
                default:
                    if (value instanceof String) {
                        mapColumn.put(field.getName(), (String) value);
                    } else {
                        mapColumn.put(field.getName(), String.valueOf(value));
                    }
                    BeanUtil.invokSetMethod(v2, field.getName(), value, field.getType());
                    break;
            }

        }
        // 将数据写入table
        Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
        mapFamily.put(CalcNodeConstant.TABLE_CF_ONE, mapColumn);

        table.writeColumnsByRow(rowKey, mapFamily);
        return (R) v2;
    }

}
