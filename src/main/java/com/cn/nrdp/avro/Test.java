package com.cn.nrdp.avro;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/29
 * @since 1.0.0
 */

public class Test {


    public static void main(String[] args) throws IOException {
        // `from_avro` requires Avro schema in JSON string format.
        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/user.avsc")));
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("sql test")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "spark01:9092,spark02:9092,spark03:9092")
                .option("subscribe", "eztest12")
                .load();

        // 1. Decode the Avro data into a struct;
// 2. Filter by column `favorite_color`;
// 3. Encode the column `name` in Avro format.
        Dataset<Row> output = df
//            .select(from_avro(col("value"), jsonFormatSchema).as("user"))
                .select("value").as("user");
//            .where("user.favorite_color == \"red\"");
//            .select(to_avro(col("user.name")).as("value"));

        StreamingQuery query = output
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "spark01:9092,spark02:9092,spark03:9092")
                .option("topic", "eztest12")
                .start();
    }
}
