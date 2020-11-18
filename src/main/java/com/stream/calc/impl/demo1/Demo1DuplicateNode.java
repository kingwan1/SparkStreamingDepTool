/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import java.text.ParseException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.Demo1Message;
import com.stream.calc.common.DuplicateCalcNode;
import com.stream.constant.ChargeConstant;
import com.stream.util.DateUtil;

/**
 * Demo1DuplicateNode  唯一主键rowkey拼接去重
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1DuplicateNode extends DuplicateCalcNode<Demo1Message, Demo1Message> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(Demo1DuplicateNode.class);
    /**
     * @param calcNodeParams
     */
    public Demo1DuplicateNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
        // TODO Auto-generated constructor stub
    }

    /* 拼接去重rowkey
     */
    @Override
    public String setKey(Demo1Message v2) {
        String dateStr = DateUtil.format(new Date(), ChargeConstant.DATE_FORMAT_YYYYMMDD);
        try {
            dateStr = DateUtil.format(v2.getClkTime(), DateUtil.DEFAULT_FORMAT, DateUtil.YYYYMMDD);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            logger.error(
                    String.format("process Processing time conversion exceptions, exception is：%s", e.getMessage()), e);
        }

        // 拼接rowkey
        String rowKey = String.format(ChargeConstant.ZSYX_TABLE_ROWKEY_DUP_PATTERN,
                ChargeConstant.ZSYX_TABLE_ROWKEY, dateStr, v2.getMachineId(), v2.getSerialNum());
        return rowKey;
    }

}
