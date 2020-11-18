/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * json相关的转换工具
 * 
 * @author wzs
 * @date 2018-03-15
 */
public class JsonUtil {

    /**
     * 将map转成String类型的json
     * 
     * @param map
     * @return 返回string类型的json
     */
    public static String getMap2Json(HashMap<String, String> map) {
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        return gson.toJson(map);
    }

    /**
     * 将json字符串转成Map
     * 
     * @param json
     * @return
     */
    public static HashMap<String, String> getJson2Map(String json) {
        GsonBuilder gb = new GsonBuilder();
        Gson g = gb.create();
        Type type = new TypeToken<HashMap<String, String>>() {

            /**
             * 生成序列号
             */
            private static final long serialVersionUID = 1L;
        }.getType();

        HashMap<String, String> map = g.fromJson(json, type);
        return map;
    }

    /**
     * 将list转成 String
     * 
     * @param list
     * @param separator 分隔符
     * @return 返回list的String
     */
    public static String getList2String(List<String> list, String separator) {
        StringBuilder sb = new StringBuilder();
        for (String s : list) {
            if (s != null && !"".equals(s)) {
                sb.append(separator).append(s);
            }
        }
        return sb.toString();
    }

    /**
     * str转成list
     * 
     * @param str
     * @param separator
     * @return 返回string分割后的list
     */
    public static List<String> getString2List(String str, String separator) {
        if (null == str) {
            return new ArrayList<String>();
        }
        List<String> list1 = new ArrayList<String>();
        Collections.addAll(list1, str.split(separator));
        List<String> list2 = new ArrayList<String>();
        list2.add("");
        list1.removeAll(list2);
        return list1;
    }

    public static void main(String[] args) {
        List<String> list = JsonUtil.getString2List("test,asdf,", ",");
        System.out.println(list.toString());
    }

}