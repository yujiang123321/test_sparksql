package com.cn.nrdp.main;

import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2018/11/19
 * @since 1.0.0
 */

public class PropertieUtils {
    public static Properties getProperties(String LoPath){
        Properties properties = new Properties();
        File file = new File(LoPath);
        if (file.exists()&&file.isFile()){
            try {

                properties.load(new FileInputStream(new File(LoPath)));
                return properties;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            InputStream LoPathProperties = PropertieUtils.class.getClassLoader().getResourceAsStream(LoPath);
            try {
                properties.load(LoPathProperties);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return properties;
        }
        return null;
    }

    public static Set<Tuple2<String,String>> getPropertiesToList(String propertiesPath){
        Set<Tuple2<String,String>> tuple2List = new HashSet<Tuple2<String, String>>();
        Properties properties = getProperties(propertiesPath);
        for (Map.Entry entry: properties.entrySet()){
            tuple2List.add(new Tuple2<String, String>(entry.getKey().toString(),entry.getValue().toString()));
        }
        return tuple2List;
    }
}
