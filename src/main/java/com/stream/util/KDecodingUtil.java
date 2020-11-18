/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;


import org.apache.commons.lang.StringUtils;

/**
 * k域解析
 * 
 * @author wzs
 * @date 2018-03-29
 */
public class KDecodingUtil {
    public static final String CON = "=";
    public static final String SEP = "\0";
    public static final String NULL = null;

    public static String evaluate(String k, String key) throws Exception {
        if (StringUtils.isBlank(k)) {
            return NULL;
        }
        String deRes = StringBases64.bcBase64Dec(k);
        if (StringUtils.isEmpty(deRes)) {
            return NULL;
        }

        return getVByK(deRes, key);
    }

    public static String getVByK(String source, String key) {
        
        String value = NULL;
        String keyParam = key + "=";
        int keyLen = keyParam.length();
        
        if (StringUtils.isBlank(key)) {
            return NULL;
        }
        // 按$把k域值分隔成包含多个键值对（如click=1）的数组，遍历数组
        String[] kSplitStrs = source.split("\\" + SEP);
        for (String s : kSplitStrs) {
            // 对每个遍历的值再按= 号分隔成key和value
            String[] kv = s.split(CON);
            if (kv.length > 0) {
                // 依次和入参的key值比较,截取匹配的键值对
                if (key.equals(String.valueOf(kv[0]))) {
                    value = s.substring(keyLen);
                    break;
                }  
            }                       
        }
        return value;
    }
}