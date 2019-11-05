package com.cn.nrdp.sparksqlapi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/2
 * @since 1.0.0
 */

public class createDataframe {
    private static List list = new ArrayList();
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]").setAppName("test");
        SparkSession sparkSession = SparkSession.builder().appName("createDataframe").config(sparkConf).getOrCreate();
        Dataset<Long> range = sparkSession.range(1, 50, 10);
        range.show();
//        list.add("\"Alex\", \"浙江\", 39, 230.00");
//        list.add("\"Bob\", \"北京\", 18, 170.00");
//        Dataset<Row> dataFrame = sparkSession.createDataFrame(list, Peoinformation.class);
//        dataFrame.printSchema();

    }
}
