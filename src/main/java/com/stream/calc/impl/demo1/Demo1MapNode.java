/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.calc.impl.demo1;

import java.text.ParseException;

import org.apache.log4j.Logger;

import com.stream.bean.CalcNodeParams;
import com.stream.bean.Demo1;
import com.stream.bean.Demo1Message;
import com.stream.calc.common.MapCalcNode;
import com.stream.constant.ChargeConstant;
import com.stream.util.DateUtil;
import com.stream.util.KDecodingUtil;
import com.stream.util.StringUtil;

/**
 * Demo1MapNode
 * 
 * @author wzs
 * @date 2018-05-28
 */
public class Demo1MapNode extends MapCalcNode<Demo1Message, Demo1> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(Demo1MapNode.class);
    /**
     * @param calcNodeParams
     */
    public Demo1MapNode(CalcNodeParams calcNodeParams) {
        super(calcNodeParams);
    }

    /*
     * 初始化 bean计算
     * 
     */
    @Override
    public Demo1 process(Demo1Message v) throws Exception {
        Demo1 demo1 = new Demo1();
        demo1.setAcctId(v.getUserId());
        demo1.setContractlineId(v.getOrderRow());
        demo1.setClk(1);
        try {
            demo1.setClkTimeDay(
                    DateUtil.format(v.getClkTime(), DateUtil.DEFAULT_FORMAT, DateUtil.YYYYMMDD));
            demo1.setClkTimeHour(
                    DateUtil.format(v.getClkTime(), DateUtil.DEFAULT_FORMAT, DateUtil.HH));
        } catch (ParseException e) {
            logger.error(String.format("%s exception, the exception information is: %s", this.getClass().getName(),
                    e.getMessage()), e);
        }
        double price = StringUtil.stringToDouble(v.getChgPrice());
        double tmpCsm = price * ChargeConstant.INT_NUMBER + 0.5;
        demo1.setCsm(new Double(tmpCsm).longValue());
        double tmpCash = price * ChargeConstant.INT_NUMBER * v.getRrate() + 0.5;
        demo1.setCash(new Double(tmpCash).longValue());
        // 判断 srcId > 10000 是mobile 否则是pc
        long srcId = 0;
        String cjStr = v.getCj();
        if (null != cjStr && !"".equals(cjStr)) {
            String cjValue = StringUtil.cjDecoder(cjStr, ChargeConstant.CJ_K_KEY);
            String srcIdStr = KDecodingUtil.evaluate(cjValue, ChargeConstant.ZSYX_K_SRC_ID);
            if (null != srcIdStr && !"".equals(srcIdStr)) {
                srcId = Long.parseLong(srcIdStr);
            }
        }

        if (srcId > 10000) {
            // mobile
            demo1.setMobileCash(demo1.getCash());
            demo1.setMobileClk(demo1.getClk());
            demo1.setMobileCsm(demo1.getCsm());
            demo1.setPcCash(0);
            demo1.setPcClk(0);
            demo1.setPcCsm(0);
        } else {
            // pc
            demo1.setMobileCash(0);
            demo1.setMobileClk(0);
            demo1.setMobileCsm(0);
            demo1.setPcCash(demo1.getCash());
            demo1.setPcClk(demo1.getClk());
            demo1.setPcCsm(demo1.getCsm());
        }

        return demo1;
    }

}
