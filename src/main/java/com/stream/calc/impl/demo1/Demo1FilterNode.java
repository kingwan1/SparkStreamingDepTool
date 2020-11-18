/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import com.stream.bean.CalcNodeParams;
import com.stream.calc.common.FilterCalcNode;
import com.stream.constant.ChargeConstant;

/**
 * ZsyxClkCsmMessageFilterNode 空行或非法行过滤
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1FilterNode extends FilterCalcNode<String, String> {
    private static final long serialVersionUID = 1L;

    /**
     * @param calcNodeParams
     */
    public Demo1FilterNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /*
     * 过滤非法line 以及非数据
     * 
     */
    @Override
    public Boolean process(String str) {
        boolean flag = false;
        if (str != null) {
            String[] fields = str.split(ChargeConstant.PACK_FIELD_SEPARATOR);
            if (fields.length >= ChargeConstant.ZSYX_CLK_CSM_FIELD_LENGTH) {
                // 剔除 非数据  (appid=317,chgtype=1)
                if (ChargeConstant.ZSYX_CLK_APP_ID.equals(fields[2])
                        && ChargeConstant.ZSYX_CLK_CHG_TYPE.equals(fields[6])
                        && ChargeConstant.ZSYX_CLK_CHG_TAG.equals(fields[0])) {
                    flag = true;
                }
            }
        }
        return flag;
    }

}
