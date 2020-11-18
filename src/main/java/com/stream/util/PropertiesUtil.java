package com.stream.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author wzs
 * @version 1.0
 * @title PropertiesUtil
 * @description PropertiesUtil
 * @date 20/3/5
 */
public class PropertiesUtil {

    public static Properties load(String fileName) {
        if (fileName == null || fileName.trim().length() == 0) {
            return null;
        }
        Properties p = new Properties();
        try {
            InputStream is = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
            if (is != null) {
                p.load(is);
                is.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }
}
