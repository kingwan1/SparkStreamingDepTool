/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.common;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.stream.bean.CalcNodeParams;
import com.stream.constant.CalcNodeConstant;
import com.stream.exception.TableException;
import com.stream.util.BeanUtil;
import com.stream.util.TableUtil;

import table.java.api.Cell;

/**
 * calcNode 去重算子操作类
 * 
 * @author wzs
 * @date 2018-03-14
 */
public abstract class DuplicateCalcNode<T, R> extends FilterCalcNode<T, R> {
    private static final long serialVersionUID = 1L;
    // 初始化table
    private TableUtil table = null;

    /**
     * 构造
     * 
     * @param calcNodeParams
     */
    public DuplicateCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    /**
     * 去重设置计算的key值组合
     * 
     * @param t 要进行去重的实例对象
     * @return String 去重的key
     */
    public abstract String setKey(T t);

    /**
     * 去重操作，根据rowkey在table去重; 如果第一次出现写入table，否则 在table标记重复次数
     * 
     * @param v2 去重对象
     * @return 没有重复返回true,否则false
     * @throws TableException
     */
    public Boolean process(T v2) throws Exception {
        boolean flag = false;
        // 拼接rowkey
        String rowKey = this.setKey(v2);
        // table交互去重，如果出现重复加1，同时设置返回值为false
        if (null == this.table) {
            this.table = new TableUtil();
        }
        Cell cellNum =
                this.table.selectCell(CalcNodeConstant.TABLE_CF_ONE, CalcNodeConstant.DUP_TABLE_COLUMN_NUM, rowKey);
        Map<String, Object> mapColumn = new HashMap<String, Object>();
        if (null != cellNum) {
            // 重复出现加1
            int tempCellClk = Integer.parseInt(new String(cellNum.getValue())) + 1;
            mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, String.valueOf(tempCellClk));
        } else {
            // 首次出现写在table
            mapColumn.put(CalcNodeConstant.DUP_TABLE_COLUMN_NUM, CalcNodeConstant.DUPLICATE_NUM);
            Field[] fields = v2.getClass().getDeclaredFields();
            // 将bean写入table
            for (Field field : fields) {
                Object value = BeanUtil.invokGetMethod(v2, field.getName());
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
            flag = true;
        }
        Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
        mapFamily.put(CalcNodeConstant.TABLE_CF_ONE, mapColumn);
        table.writeColumnsByRow(rowKey, mapFamily);

        return flag;
    }

}
