/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * 字符串转换工具
 * 
 * @author wzs
 * @date 2018-03-29
 */
public class StringUtil {

    /**
     * cjdecode 函数
     * 
     * @param cjStr
     * @return 返回map
     */
    public static String cjDecoder(String cj, String key) {
        if (StringUtils.isBlank(cj)) {
            // cj can\'t be null or \'\'
            return null;
        }
        if (StringUtils.isBlank(key)) {
            // key is null or \'\',return cj
            return cj;
        }

        Map<String, String> cjMap = getCjMap(cj);
        return (String) cjMap.get(key);
    }

    /**
     * cjdecode 解析函数
     * 
     * @param cj
     * @return
     */
    private static Map<String, String> getCjMap(String cj) {
        HashMap<String, String> cjMap = new HashMap<String, String>();
        String[] cjList = cj.split("\\|");
        for (String kv : cjList) {
            String[] kvList = kv.split("=");
            if (kvList.length == 1) {
                cjMap.put(kvList[0], null);
            } else {
                cjMap.put(kvList[0], kvList[1]);
            }
        }

        return cjMap;
    }

    /**
     * 字符型数字转成BigDecimal
     * 
     * @param str
     * @return 返回BigDecimal对象
     */
    public static BigDecimal stringToBigDecimal(String str, int num) {
        BigDecimal priceBigDeci = null;
        if (null != str && !"".equals(str)) {

            priceBigDeci = new BigDecimal(str);
        } else {
            priceBigDeci = new BigDecimal(0);
        }
        priceBigDeci.setScale(num, BigDecimal.ROUND_HALF_UP);
        return priceBigDeci;
    }

    /**
     * 将字符串转成double 类型数字
     * 
     * @param str
     * @return
     */
    public static double stringToDouble(String str) {
        if (str != null && !"".equals(str)) {
            return Double.parseDouble(str);
        } else {
            return 0.0;
        }
    }

    /**
     * 判断字符串是否为空
     * 
     * @param str
     * @return
     */
    public static boolean isNullStr(String str) {
        if (str == null || "".equals(str)) {
            return true;
        } else {
            return false;
        }
    }

}
