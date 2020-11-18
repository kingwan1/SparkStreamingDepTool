/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Class BeanUtil
 * 
 * @author wzs
 * @date 2018-05-16
 */
public class BeanUtil {
    public static final String BEAN_GET_FLAG = "get";
    public static final String BEAN_SET_FLAG = "set";
    public static final String FIELD_SEPARATOR = "\t";
    public static final String FIELD_KEY_VALUE_SEPARATOR = ":";

    /**
     * 获取bean对象的get方法值
     * 
     * @param obj 赋值对象
     * @param fieldName 字段名字
     * @return 字段对应的value
     * @throws Exception
     */
    public static Object invokGetMethod(Object obj, String fieldName) throws Exception {
        Class<? extends Object> objClass = obj.getClass();
        Method[] tmpMethods = objClass.getMethods();
        String tmpFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        Object tmpObject = "";
        for (Method tmpMethod : tmpMethods) {
            if (tmpMethod.getName().equals(BEAN_GET_FLAG + tmpFieldName)) {
                tmpObject = tmpMethod.invoke(obj);
            }
        }
        return tmpObject;
    }

    /**
     * 为Bean对象set赋值
     * 
     * @param obj 赋值对象
     * @param fieldName 赋值字段名字
     * @param value 赋值value
     * @throws Exception
     */
    public static void invokSetMethod(Object obj, String fieldName, Object value, Class<?> valueType) throws Exception {
        Class<? extends Object> objClass = obj.getClass();
        String tmpmethodName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        Method method = objClass.getMethod(BEAN_SET_FLAG + tmpmethodName, valueType);
        method.invoke(obj, value);
    }

    /**
     * 将message 转成 Bean实例
     * 
     * @param str message信息
     * @param separator message信息分隔符 默认 tab 分隔符
     * @param cls 转成对应的Bean类型
     * @param strs Bean与message对应的字段顺序 {"key:value","key:value"} key为bean的field字段，value 是message对应index 从0开始；
     * @return Bean实例
     * @throws Exception 转换异常
     */
    public static Object string2Bean(String str, String separator, Class<?> cls, String[] strs) throws Exception {
        if (null == separator) {
            separator = FIELD_SEPARATOR;
        }
        Object tmpObj = cls.newInstance();
        Field[] fields = cls.getDeclaredFields();

        if (null != str && !"".equals(str)) {
            String[] fieldMessages = str.split(separator);
            if (fieldMessages.length >= strs.length) {
                for (int i = 0; i < strs.length; i++) {
                    String fieldNameIndex = strs[i];
                    String[] fieldNameIndexs = fieldNameIndex.split(FIELD_KEY_VALUE_SEPARATOR);
                    String fieldName = fieldNameIndexs[0];
                    int index = Integer.parseInt(fieldNameIndexs[1]);
                    for (Field field : fields) {
                        if (field.getName().equals(fieldName)) {
                            switch (field.getType().toString()) {
                                case "byte":
                                    invokSetMethod(tmpObj, fieldName, Byte.parseByte(fieldMessages[index]), byte.class);
                                    break;
                                case "short":
                                    invokSetMethod(tmpObj, fieldName, Short.parseShort(fieldMessages[index]),
                                            short.class);
                                    break;
                                case "boolean":
                                    invokSetMethod(tmpObj, fieldName, Boolean.parseBoolean(fieldMessages[index]),
                                            boolean.class);
                                    break;
                                case "long":
                                    invokSetMethod(tmpObj, fieldName, Long.parseLong(fieldMessages[index]), long.class);
                                    break;
                                case "double":
                                    invokSetMethod(tmpObj, fieldName, Double.parseDouble(fieldMessages[index]),
                                            double.class);
                                    break;
                                case "float":
                                    invokSetMethod(tmpObj, fieldName, Float.parseFloat(fieldMessages[index]),
                                            float.class);
                                    break;
                                case "int":
                                    invokSetMethod(tmpObj, fieldName, Integer.parseInt(fieldMessages[index]),
                                            int.class);
                                    break;
                                default:
                                    invokSetMethod(tmpObj, fieldName, fieldMessages[index], String.class);
                                    break;
                            }

                        }
                    }
                }

            } else {
                throw new Exception("message field length < fieldNames length Error...");
            }
        }
        return tmpObj;
    }

    /**
     * 将message 转成 Bean实例
     * 
     * @param str message信息
     * @param separator message信息分隔符 默认tab分隔符
     * @param tmpObj 实例赋值对象
     * @param strs Bean与message对应的字段顺序 {"key:value","key:value"} key为bean的field字段，value 是message对应index 从0开始；
     * @return Bean实例
     * @throws Exception 转换异常
     */
    public static Object string2Bean(String str, String separator, Object tmpObj, String[] strs) throws Exception {
        if (null == separator) {
            separator = FIELD_SEPARATOR;
        }
        Field[] fields = tmpObj.getClass().getDeclaredFields();

        if (null != str && !"".equals(str)) {
            String[] fieldMessages = str.split(separator);
            if (fieldMessages.length >= strs.length) {
                for (int i = 0; i < strs.length; i++) {
                    String fieldNameIndex = strs[i];
                    String[] fieldNameIndexs = fieldNameIndex.split(FIELD_KEY_VALUE_SEPARATOR);
                    String fieldName = fieldNameIndexs[0];
                    int index = Integer.parseInt(fieldNameIndexs[1]);
                    for (Field field : fields) {
                        if (field.getName().equals(fieldNameIndexs[0])) {
                            switch (field.getType().toString()) {
                                case "byte":
                                    invokSetMethod(tmpObj, fieldName, Byte.parseByte(fieldMessages[index]), byte.class);
                                    break;
                                case "short":
                                    invokSetMethod(tmpObj, fieldName, Short.parseShort(fieldMessages[index]),
                                            short.class);
                                    break;
                                case "boolean":
                                    invokSetMethod(tmpObj, fieldName, Boolean.parseBoolean(fieldMessages[index]),
                                            boolean.class);
                                    break;
                                case "long":
                                    invokSetMethod(tmpObj, fieldName, Long.parseLong(fieldMessages[index]), long.class);
                                    break;
                                case "double":
                                    invokSetMethod(tmpObj, fieldName, Double.parseDouble(fieldMessages[index]),
                                            double.class);
                                    break;
                                case "float":
                                    invokSetMethod(tmpObj, fieldName, Float.parseFloat(fieldMessages[index]),
                                            float.class);
                                    break;
                                case "int":
                                    invokSetMethod(tmpObj, fieldName, Integer.parseInt(fieldMessages[index]),
                                            int.class);
                                    break;
                                default:
                                    invokSetMethod(tmpObj, fieldName, fieldMessages[index], String.class);
                                    break;
                            }

                        }
                    }
                }

            } else {
                throw new Exception("message field length < fieldNames length Error...");
            }
        }
        return tmpObj;
    }

}
