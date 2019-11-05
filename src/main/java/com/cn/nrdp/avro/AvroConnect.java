//package com.cn.nrdp.avro;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.avro.*;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
///**
// * 〈一句话功能简述〉<br>
// * 〈〉
// *
// * @author yujiang
// * @create 2019/1/29
// * @since 1.0.0
// */
//
//public class AvroConnect {
//    public static void main(String[] args) throws IOException {
//        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/user.avsc")));
//        SparkSession sparkSession = SparkSession.builder()
//                .master("local[2]")
//                .config(new SparkConf())
//                .getOrCreate();
//
//        //获取kafka 外部数据源
//        Dataset<Row> kafkaDF = sparkSession.readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "spark01:9092,spark02:9092,spark03:9092")
//                .option("subscribe", "eztest12")
//                .load();
//
//
////        Dataset<Row> output = kafkaDF
////                .select(from_avro(col("value"), jsonFormatSchema).as("user")
////                .where("user.favorite_color == \"red\"")
////                .select("user.name").as("value");
//
//        output.writeStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "spark01:9092,spark02:9092,spark03:9092")
//                .option("topic", "eztest10")
//                .start();
//
//
//
//    }
//}
