package com.stream.calc.common;

import java.util.HashMap;
import java.util.Map;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.MapCalcNode;
import com.stream.constant.CalcNodeConstant;
import com.stream.util.TableUtil;

/**
 * @author wzs
 * @version 1.0
 * @title OutPutMapCalcNode
 * @description OutPutMapCalcNode
 * @date 20/5/25
 */
public abstract class OutPutMapCalcNode extends MapCalcNode<Map<String, Object>, Map<String, Object>> {
    private static final long serialVersionUID = 1L;

    // 算子调用时候的传入参数bean
    @SuppressWarnings("unused")
    private CalcNodeParams calcNodeParams;

    // 初始化table
    private TableUtil table = null;

    /**
     * @param calcNodeParams 构造器使用定义参数
     */
    public OutPutMapCalcNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        this.calcNodeParams = calcNodeParams;

    }

    /**
     * 去重设置计算的key值组合
     * 
     * @param t 要进行去重的实例对象
     * @return String 去重的key
     */
    public abstract String setKey(Map<String, Object> v);

    /*
     * (non-Javadoc) 继承父类的方法。
     * 
     */
    @Override
    public Map<String, Object> process(Map<String, Object> v) throws Exception {
        // table交互去重，如果出现重复加1，同时设置返回值为false
        if (null == this.table) {
            this.table = new TableUtil();
        }
        String rowKey = setKey(v);
        Map<String, Object> mapColumn = new HashMap<String, Object>();
        Map<String, Map<String, Object>> mapFamily = new HashMap<String, Map<String, Object>>();
        mapFamily.put(CalcNodeConstant.TABLE_CF_ONE, mapColumn);
        table.writeColumnsByRow(rowKey, mapFamily);

        return v;
    }

}
