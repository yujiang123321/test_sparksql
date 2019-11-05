package com.cn.nrdp.hive;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/23
 * @since 1.0.0
 */

public class hiveExample {

    // $example on:spark_hive$
    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession hiveSparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("java hive example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();


//        hiveSparkSession.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
//        hiveSparkSession.sql("load data local inpath '/Users/yujiang/IdeaProjects/sparksql/src/main/resources/kv1.txt' into table src");
//        hiveSparkSession.sql("drop table src");

        Dataset<Row> dataset = hiveSparkSession.sql("select * from src where key < 10 order by key");
//        dataset.show();
//
//        hiveSparkSession.sql("select count(*) from src").show();

        Dataset<String> map = dataset.map(new MapFunction<Row, String>() {
            public String call(Row value) throws Exception {
                return "key : " + value.get(0) +"value : " + value.get(1);
            }
        }, Encoders.STRING());

        List<Record> recordList = new ArrayList<Record>();
        for (int i=0;i<=100;i++){
            Record record = new Record();
            record.setKey(i);
            record.setValue("val_"+i);
            recordList.add(record);
        }

        Dataset<Row> dataFrame = hiveSparkSession.createDataFrame(recordList, Record.class);
        dataFrame.createOrReplaceTempView("record");

        hiveSparkSession.sql("select * from src,record where src.key=record.key ").show();




    }
}
