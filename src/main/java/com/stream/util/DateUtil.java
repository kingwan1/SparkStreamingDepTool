/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期转换
 * 
 * @author wzs
 * @date 2018-03-29
 */
public class DateUtil {

    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String YYYYMMDD = "yyyyMMdd";

    public static final String YYYYMMDDHH = "yyyyMMddHH";

    public static final String YYYYMMDD_HH = "yyyyMMdd_HH";

    public static final String HH = "HH";

    public static final String MIN = "mm";

    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    public static final String YYYYMMDDHHMM = "yyyyMMddHHmm";

    /**
     * 获取当前时间，并返回指定格式的时间字符串。
     * 
     * @param format
     * @return String
     */
    public static String format(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 格式化时间
     * 
     * @param format
     * @return
     * @throws java.text.ParseException
     */
    public static String format(String srcDate, String srcFormat, String destFormat) throws ParseException {
        SimpleDateFormat src = new SimpleDateFormat(srcFormat);
        Date date = src.parse(srcDate);
        SimpleDateFormat sdf = new SimpleDateFormat(destFormat);
        return sdf.format(date);
    }

    /**
     * 将字串转成日期和时间
     * 
     * @param date
     * @param format
     * @return Date
     * @throws java.text.ParseException
     */
    public static Date convertStrToDate(String date, String format) throws ParseException {
        DateFormat formatter = new SimpleDateFormat(format);
        return formatter.parse(date);
    }

    /**
     * 返回传入时间与当前系统时间天数的差，工作台任务逻辑处理用
     * 
     * @param v
     * @return int
     * @throws ParseException
     */
    public static int datediff(String v, String format) throws ParseException {
        Date date = new Date();
        String currentTime = DateUtil.format(date, DateUtil.YYYYMMDD);
        Date currentDate = DateUtil.convertStrToDate(currentTime, DateUtil.YYYYMMDD);
        long curentMin = currentDate.getTime();
        Date dtTemp = DateUtil.convertStrToDate(v, format);
        long dtParam = dtTemp.getTime();
        long diff = dtParam - curentMin;
        int days = (int) (diff / (1000 * 60 * 60 * 24));
        return days;
    }
}
