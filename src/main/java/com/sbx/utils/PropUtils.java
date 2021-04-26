package com.sbx.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 *加载配置文件
 */
public class PropUtils {
   public static Properties getProperties(String propName){
       Properties prop = new Properties();
       InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream(propName);
       try {
           prop.load(inputStream);
       } catch (IOException e) {
           e.printStackTrace();
       }
       return prop;
   }
}
