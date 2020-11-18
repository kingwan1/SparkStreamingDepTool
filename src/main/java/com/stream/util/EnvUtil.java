/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.util.List;
import java.util.Map;

import com.stream.bean.YarmEnvBean;

/**
 * 
 * Description: 环境的工具类,可以获取配置环境 
 *
 * @author wzs
 * @date 2018-03-08
 * @Version 1.0.0
 */
public class EnvUtil {
    // 定义yarn参数实体
    private static YarmEnvBean yarmBean;
    // 文件路径
    private static String filePath;

//    /**
//     * 加装环境配置
//     * 
//     * @param path 文件路径
//     * @return EnvUtil对象
//     */
//    public static EnvUtil loadEvnYamlFile(String path) {
//        filePath = path;
//        yarmBean = (YarmEnvBean) ConfFileUtil.readYarmFile(path, YarmEnvBean.class);
//        return new EnvUtil();
//    }
//
//    /**
//     * 读取yamlbean实例
//     * 
//     * @param path 文件路径
//     * @return yarmBean实体
//     */
//    public static YarmEnvBean getEvnYamlBean() {
//        if (null == yarmBean) {
//            return (YarmEnvBean) ConfFileUtil.readYarmFile(filePath, YarmEnvBean.class);
//        }
//
//        return yarmBean;
//    }
//
//    /**
//     * 获取param 环境参数，返回string类型
//     * 
//     * @param path 文件路径
//     * @param key 获取value的key参数
//     * @return value值
//     */
//    public static String getStringParams(String key) {
//        YarmEnvBean bean = getEvnYamlBean();
//        Map<String, Object> map = bean.getParams();
//        Object obj = map.get(key);
//        return obj.toString();
//    }
//
//    /**
//     * 获取环境param参数，返回 int类型
//     * 
//     * @param path 文件路径
//     * @param key 获取value的key参数
//     * @return
//     */
//    public static int getIntParams(String key) {
//        String value = getStringParams(key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        } else {
//            return Integer.parseInt(value);
//        }
//    }
//
//    /**
//     * 获取bigpipe 环境参数
//     * 
//     * @param path 文件路径
//     * @param index 取第index个bigpipe环境参数列表
//     * @return Map<String,Object> BigPipe环境参数
//     */
//    public static Map<String, Object> getBigPipe(int index) {
//        YarmEnvBean bean = getEvnYamlBean();
//        List<Map<String, Object>> listBigPipe = bean.getBigPipe();
//        return listBigPipe.get(index);
//    }
//
//    /**
//     * 获取bigpie 环境参数，取第0个 bigpipe的环境参数
//     * 
//     * @param path
//     * @return Map<String,Object> BigPipe环境参数
//     */
//    public static Map<String, Object> getBigPipe() {
//        return getBigPipe(0);
//    }
//
//    /**
//     * 获取bigpipe 环境参数，返回string类型
//     * 
//     * @param path 文件路径
//     * @param index 第index个map集合
//     * @param key 获取value的key参数
//     * @return Map<String,Object> BigPipe环境参数
//     */
//    public static String getStringBigPipe(int index, String key) {
//        Map<String, Object> map = getBigPipe(index);
//        Object val = map.get(key);
//        return val.toString();
//    }
//
//    /**
//     * 获取bigpipe 第一个map环境参数，返回string类型
//     * 
//     * @param path 文件路径
//     * @param index 获取第index个map集合参数
//     * @param key 获取value的key参数
//     * @return  返回key 对应的value
//     */
//    public static String getStringBigPipe(String key) {
//        return getStringBigPipe(0, key);
//    }
//
//    /**
//     * 获取bigpipe 环境参数，返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 获取第index个map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntBigPipe(int index, String key) {
//        String value = getStringBigPipe(index, key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        }
//        return Integer.parseInt(value);
//    }
//
//    /**
//     * 获取bigpipe 环境参数，返回int类型 默认取第一个map
//     * 
//     * @param path 文件路径
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntBigPipe(String key) {
//        return getIntBigPipe(0, key);
//    }
//
//    /**
//     * 获取zookeeper 环境参数
//     * 
//     * @param path 文件路径
//     * @param index 取第index个zookeeper环境参数列表
//     * @return 返回Map<String, Object> 环境参数
//     */
//    public static Map<String, Object> getZookeeper(int index) {
//        YarmEnvBean bean = getEvnYamlBean();
//        List<Map<String, Object>> listZookeeper = bean.getZookeeper();
//        return listZookeeper.get(index);
//    }
//
//    /**
//     * 获取zookeeper 环境参数，取第1个 zookeeper的环境参数
//     * 
//     * @param path 文件路径
//     * @return 返回Map<String, Object> 环境参数
//     */
//    public static Map<String, Object> getZookeeper() {
//        return getZookeeper(0);
//    }
//
//    /**
//     * 获取zookeeper 环境参数，返回String
//     * 
//     * @param path 文件路径
//     * @param index 获取index位置的map参数集合
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static String getStringZookeeper(int index, String key) {
//        Map<String, Object> map = getZookeeper(index);
//        Object obj = map.get(key);
//        return obj.toString();
//    }
//
//    /**
//     * 获取zookeeper 第一个map环境参数，返回String
//     * 
//     * @param path 文件路径
//     * @param index 获取index位置的map集合
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static String getStringZookeeper(String key) {
//        return getStringZookeeper(0, key);
//    }
//
//    /**
//     * 获取zookeeper 环境参数，返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 获取index位置的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntZookeeper(int index, String key) {
//        String value = getStringZookeeper(index, key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        } else {
//            return Integer.parseInt(value);
//        }
//    }
//
//    /**
//     * 获取zookeeper的参数
//     * 
//     * @param path 文件路径
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntZookeeper(String key) {
//        return getIntZookeeper(0, key);
//    }
//
//    /**
//     * 获取table 环境参数
//     * 
//     * @param path 文件路径
//     * @param index 取第index个Table环境参数列表
//     * @return 返回Map<String, Object> 环境参数
//     */
//    public static Map<String, Object> getTable(int index) {
//        YarmEnvBean bean = getEvnYamlBean();
//        List<Map<String, Object>> listTable = bean.getTable();
//        return listTable.get(index);
//    }
//
//    /**
//     * 获取table 环境参数，取第1个 map的环境参数
//     * 
//     * @param path 文件路径
//     * @return 返回Map<Stirng, Object> 环境参数
//     */
//    public static Map<String, Object> getTable() {
//        return getTable(0);
//    }
//
//    /**
//     * 获取table 环境参数, 返回String 类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个Table环境参数列表
//     * @return 返回key对应的value
//     */
//    public static String getStringTable(int index, String key) {
//        Map<String, Object> map = getTable(index);
//        Object obj = map.get(key);
//        String value = obj.toString();
//        return value;
//    }
//
//    /**
//     * 获取table 环境参数, 返回String 类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个Table环境参数列表
//     * @return 返回key对应的value
//     */
//    public static String getStringTable(String key) {
//        return getStringTable(0, key);
//    }
//
//    /**
//     * 获取table参数,返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntTable(int index, String key) {
//        String value = getStringTable(index, key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        } else {
//            return Integer.parseInt(value);
//        }
//    }
//
//    /**
//     * 获取table参数,第一个map,返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntTable(String key) {
//        return getIntTable(0, key);
//    }
//
//    /**
//     * 获取hdfs 环境参数
//     * 
//     * @param path 文件路径
//     * @param index 取第index个Hdfs环境参数列表
//     * @return
//     */
//    public static Map<String, Object> getHdfs(int index) {
//        YarmEnvBean bean = getEvnYamlBean();
//        List<Map<String, Object>> listHdfs = bean.getHdfs();
//        return listHdfs.get(index);
//    }
//
//    /**
//     * 获取hdfs 环境参数，取第0个 Table的环境参数
//     * 
//     * @param Hdfs
//     * @return 返回Map<String, Object> 环境参数
//     */
//    public static Map<String, Object> getHdfs() {
//        return getHdfs(0);
//    }
//
//    /**
//     * 获取hdfs 环境变量 返回string
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static String getStringHdfs(int index, String key) {
//        Map<String, Object> map = getHdfs(index);
//        Object obj = map.get(key);
//        return obj.toString();
//    }
//
//    /**
//     * 获取hdfs 环境变量 返回string
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static String getStringHdfs(String key) {
//        return getStringHdfs(0, key);
//    }
//
//    /**
//     * 获取Hdfs 环境变量 返回int
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntHdfs(int index, String key) {
//        String value = getStringHdfs(index, key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        } else {
//            return Integer.parseInt(value);
//        }
//    }
//
//    /**
//     * 获取Hdfs 环境变量 返回int
//     * 
//     * @param path 文件路径
//     * @param index 获取index的map集合参数
//     * @param key 获取value的key参数
//     * @return 返回key对应的value
//     */
//    public static int getIntHdfs(String key) {
//        return getIntHdfs(0, key);
//    }
//
//    /**
//     * 获取calcTemplate 环境参数
//     * 
//     * @param path 文件路径
//     * @param index 取第index个calcTemplate环境参数列表
//     * @return 返回key对应的value
//     */
//    public static Map<String, Object> getCalcTemplate(int index) {
//        YarmEnvBean bean = getEvnYamlBean();
//        List<Map<String, Object>> listTemplate = bean.getCalcTemplate();
//        return listTemplate.get(index);
//    }
//
//    /**
//     * 获取calcTemplate 环境参数，取第0个 Table的环境参数
//     * 
//     * @param calcTemplate
//     * @return 返回key对应的value
//     */
//    public static Map<String, Object> getCalcTemplate() {
//        return getCalcTemplate(0);
//    }
//
//    /**
//     * 获取calcTemplate 环境参数,返回 string类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个calcTemplate环境参数列表
//     * @return 返回key对应的value
//     */
//    public static String getStringCalcTemplate(int index, String key) {
//        Map<String, Object> map = getCalcTemplate(index);
//        Object obj = map.get(key);
//        return obj.toString();
//    }
//
//    /**
//     * 获取calcTemplate 环境参数,返回 string类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个calcTemplate环境参数列表
//     * @return 返回key对应的value
//     */
//    public static String getStringCalcTemplate(String key) {
//        return getStringCalcTemplate(0, key);
//    }
//
//    /**
//     * 获取calcTemplate 环境参数 返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个calcTemplate环境参数列表
//     * @return 返回key对应的value
//     */
//    public static int getIntCalcTemplate(int index, String key) {
//        String value = getStringCalcTemplate(index, key);
//        if (null == value || "".equals(value)) {
//            return -1;
//        } else {
//            return Integer.parseInt(value);
//        }
//    }
//
//    /**
//     * 获取calcTemplate 第一个map环境参数 返回int类型
//     * 
//     * @param path 文件路径
//     * @param index 取第index个calcTemplate环境参数列表
//     * @return 返回key对应的value
//     */
//    public static int getIntCalcTemplate(String key) {
//        return getIntCalcTemplate(0, key);
//    }

}
