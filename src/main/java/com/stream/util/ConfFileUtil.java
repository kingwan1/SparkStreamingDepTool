/**
 * Copyright (C) 2018 wzs All Rights Reserved.
 */
package com.stream.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.Yaml;

import com.stream.exception.JobConfRuntimeException;

/**
 * 配置文件解析、加装工具类
 * 
 * @author wzs
 *
 */
public class ConfFileUtil {

    /**
     * Resources目录内读取propertie文件
     * 
     * @param filePath resources内路径
     * @return properties 属性参数 k,v
     * @throws IOException
     */
    public static Properties readPropertieFile(String filePath) {
        return readPropertieFile(filePath, false);
    }

    /**
     * 绝对路径或相对路径读取Properties文件
     * 
     * @param filePath properties路径
     * @param flag 标识路径类型；true 绝对路径，全路径；false resources 相对路径下properties文件路径
     * @return 属性参数 k,v
     * @throws IOException
     */
    public static Properties readPropertieFile(String filePath, boolean flag) {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            if (flag) {
                in = new FileInputStream(filePath);
            } else {

                in = ConfFileUtil.class.getClassLoader().getResourceAsStream(filePath);
            }

            Reader bf = new BufferedReader(new InputStreamReader(in));
            prop.load(bf);
        } catch (FileNotFoundException e) {
            throw new JobConfRuntimeException("read propertiy file error", e);
        } catch (IOException e) {
            throw new JobConfRuntimeException("read propertiy file error", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new JobConfRuntimeException("propertiy file close error", e);
                }
            }
        }
        return prop;
    }

    /**
     * 读取resources 目录下的配置文件
     * 
     * @param path resource下路径
     * @param type yarm文件对应的Bean实体类
     * @return obj 实体Bean类
     * @throws FileNotFoundException
     */
    public static Object readYarmFile(String path, Class type) {
        return readYarmFile(path, type, false);
    }

    /**
     * 读取绝对路径或者resource下相对路径yarm文件
     * 
     * @param filePath
     * @param type yarm文件对于的Bean实体类
     * @param flag 标识文件路径类型，true 是绝对路径，全路径；false 是 resources相对路径
     * @return obj 实体Bean类
     */
    public static Object readYarmFile(String filePath, Class<Object> type, boolean flag) {
        InputStream in = null;
        Yaml yaml = new Yaml();
        Object ybean = null;
        try {
            if (flag) {
                in = new FileInputStream(filePath);
            } else {

                in = ConfFileUtil.class.getClassLoader().getResourceAsStream(filePath);
            }
            ybean = yaml.loadAs(in, type);
        } catch (Exception e) {
            throw new JobConfRuntimeException("read yarm file error", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new JobConfRuntimeException("yarm file close error", e);
                }
            }
        }

        return ybean;
    }

    /**
     * 读取xml配置文件，类似hadoop-site.xml文件
     * 
     * @param filePath
     * @return Conf参数
     */
    public static Configuration readConfigurationFile(String filePath) {
        Configuration conf = new Configuration();
        if (null == filePath && "".equals(filePath)) {
            return null;
        }
        conf.addResource(filePath);
        return conf;
    }

}
