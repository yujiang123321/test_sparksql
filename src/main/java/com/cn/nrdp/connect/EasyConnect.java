package com.cn.nrdp.connect;

import com.cn.nrdp.main.PropertieUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/21
 * @since 1.0.0
 */

public class EasyConnect {
    public static void main(String[] args) throws AnalysisException {
        SparkConf sparkConf = new SparkConf();
        Set<Tuple2<String, String>> initsparkconf = PropertieUtils.getPropertiesToList("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/initsparkconf");
        sparkConf.setAll(JavaConversions.asScalaSet(initsparkconf));


        //1.创建 入口 sparksession
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .appName("sql test1")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

       runBasicDataSourceExample(sparkSession);
//        runBasicParquetExample(sparkSession);
//        runParquetSchemaMergingExample(sparkSession);
//        runJsonDatasetExample(spark);
//          runJdbcDatasetExample(sparkSession);

        sparkSession.stop();
    }

    private static void runBasicDataSourceExample(SparkSession spark) throws AnalysisException {
        //在最简单的形式中，默认数据源(除非由spark.sql.sources.default配置)将用于所有操作
        Dataset<Row> usersDF = spark.read().format("parquet").load("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/users.parquet");
        usersDF.createOrReplaceTempView("users");
        usersDF.createGlobalTempView("user2");
        spark.sql("select * from users").show();

        //手动指定选项
        /**
         * 您还可以手动指定要使用的数据源以及希望传递给数据源的任何额外选项。
         * 数据源由其完全限定的名称(即但是对于内置的源，
         * 您也可以使用它们的短名称(json、parquet、jdbc、orc、libsvm、csv、text)。
         * 从任何数据源类型加载的DataFrames都可以使用此语法转换为其他类型。
         */

        //例如从json里拿 写入已parquet形式写入
//        Dataset<Row> jsonDF = spark.read().format("json").load("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.json");
//        jsonDF.select("name","age").write().format("parquet").save("jsonToparquet.parquet");


        //例如从csv中读取数据  csv是2.4后支持的
//        Dataset<Row> csvDF = spark.read().format("csv")
////                .option("sep", ";")
////                .option("inferSchema", "true")
////                .option("header", "true")
////                .load("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.csv");
////        csvDF.show();

        //例如从roc中读取数据
        /**
         * 在写操作期间也使用额外的选项。例如，您可以控制ORC数据源的bloom过滤器和字典编码。
         * 下面的ORC示例将在最喜欢的颜色上创建bloom filter，
         * 并对名称和最喜欢的颜色使用字典编码。
         * //.option("orc.row.index.stride","false" ) //创建索引？ 此处不明白什么意思 报错
         */

//        Dataset<Row> userDF = spark.read().load("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/users.parquet");
//        Dataset<Row> select = userDF.select("name", "favorite_color");
//        select.show();
//        select.write()
//                .format("orc")
//                .option("orc.compress", "ZLIB")       //高级压缩= {NONE，ZLIB，SNAPPY}
//                .option("orc.compress.size", "300200") //压缩块大小
//                .option("orc.stripe.size","67108864" ) //内存缓冲区以字节为单位进行写
//                .option("orc.row.index.stride", "10000") //索引条目之间的行数
//                .option("orc.bloom.filter.fpp", "0.05") //绽放过滤器误报率
//                .option("orc.bloom.filter.columns", "favorite_color") //逗号分隔的列名列表
//                .option("orc.dictionary.key.threshold", "1.0")//类型字段使用字典编码的阈值，如果字典中的键数大于非空行总数的这一部分，则关闭字典编码。使用1总是使用字典编码。
//                .save("namesAndFavColors.parquet");


        //与使用read API将文件加载到DataFrame并进行查询不同，您还可以使用SQL直接查询该文件。

//        spark.sql("select * from parquet.`/Users/yujiang/IdeaProjects/sparksql/src/main/resources/users.parquet`").show();

        /**
         * 保存形式：
         * SaveMode.ErrorIfExists （默认）	"error" or "errorifexists" （默认）	将DataFrame保存到数据源时，如果数据已存在，则会引发异常。
         * SaveMode.Append	"append"	将DataFrame保存到数据源时，如果数据/表已存在，则DataFrame的内容应附加到现有数据。
         * SaveMode.Overwrite	"overwrite"	覆盖模式意味着在将DataFrame保存到数据源时，如果数据/表已经存在，则预期现有数据将被DataFrame的内容覆盖。
         * SaveMode.Ignore	"ignore"	忽略模式意味着在将DataFrame保存到数据源时，如果数据已存在，则预期保存操作不会保存DataFrame的内容而不会更改现有数据。这与CREATE TABLE IF NOT EXISTSSQL中的类似。
         */
//        usersDF.write().mode(SaveMode.Append)
//                .mode(SaveMode.ErrorIfExists)
//                .mode(SaveMode.Ignore)
//                .mode(SaveMode.Overwrite)
//                .save("saveMode.parquet");



        /**
         *创建hive的持久表
         */
        Dataset<Row> jsonDF = spark.read().format("json").load("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.json");
//        jsonDF.show();


        //对于基于文件的数据源，还可以对输出进行存储和排序或分区。      分段(bucketBy)和排序仅适用于持久表：
        jsonDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("bbccdd");

        //而分区可以既使用save和saveAsTable使用DataSet API时。
//        usersDF.write().partitionBy("favorite_color")
//                .format("parquet")
//                .save("namesPartByColor.parquet");


        //可以对单个表使用分段和分区
//        usersDF
//                .write()
//                .partitionBy("favorite_color")
//                .bucketBy(42, "name")
//                .saveAsTable("people_partitioned_bucketed");

        spark.sql("select * from bbccdd").show();
//        spark.sql("select * from people_partitioned_bucketed").show();
//       spark.sql("drop table if exists people_partitioned_bucketed ");




    }

    public static void runBasicParquetExample(SparkSession sparkSession){
        Dataset<Row> jsonParquet = sparkSession.read().parquet("jsonToparquet.parquet");
        jsonParquet.write()
                .saveAsTable("JSONPARQUET");
        jsonParquet.write().option("path", "data/key=3").saveAsTable("hivetable");

//        sparkSession.sql("drop table JSONPARQUET");
//       sparkSession.sql("select * from JSONPARQUET ").show();

//        Dataset<Row> hiveparquet = sparkSession.read().parquet("/Users/yujiang/IdeaProjects/sparksql/spark-warehouse/jsonparquet");
//        hiveparquet.show();
//        hiveparquet.write().parquet("parquet.parquet");

        //此处是刷新转换表的元数据信息使其保持一致，

        //问题：麻痹老是说找不到表
    sparkSession.catalog().refreshTable("hivetable");



    }

    public static void runParquetSchemaMergingExample(SparkSession sparkSession) {
        //模式合并
        /**
         * 用户可能最终得到具有不同结果但相互兼容的模式的多个Parquet文件。Parquet数据源现在能够自动检测这种情况并合并所有这些文件的模式。
         */

        List<bean1> bean1List = new ArrayList<bean1>();
        bean1 bean1 = null;
        for (int i = 0; i <= 5; i++) {
            bean1 = new bean1();
            bean1.setValue(i);
            bean1.setAge(i * i);
            bean1List.add(bean1);
        }

        List<bean2> bean2List = new ArrayList<bean2>();
        bean2 bean2 = null;
        for (int i = 6; i <= 10; i++) {
            bean2 = new bean2();
            bean2.setValue(i);
            bean2.setAge(i * i);
            bean2List.add(bean2);
        }

//        Dataset<Row> bean1dataFrame = sparkSession.createDataFrame(bean1List, bean1.class);
//
//        bean1dataFrame.write().parquet("data/key=1");
//
//        Dataset<EasyConnect.bean2> bean2Dataset = sparkSession.
//                createDataset(Collections.singletonList(bean2), Encoders.bean(bean2.class));
//
//        bean2Dataset.write().parquet("data/key=2");


        Dataset<Row> mergeDF = sparkSession.read().option("merge.Scheme", true).parquet("data/");
        mergeDF.printSchema();
        mergeDF.show();

    }
    private static void runJdbcDatasetExample(SparkSession sparkSession){
        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/test")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "eztest5")
                .option("user", "root")
                .option("password", "YJ620622")
                .load();
        jdbcDF.show();
    }



        public static class bean1 implements Serializable {
        private int value;
        private int age;

            public int getValue() {
                return value;
            }

            public void setValue(int value) {
                this.value = value;
            }

            public int getAge() {
                return age;
            }

            public void setAge(int age) {
                this.age = age;
            }
        }

        public static class bean2 implements Serializable{
        private int value;
        private int age;

            public int getValue() {
                return value;
            }

            public void setValue(int value) {
                this.value = value;
            }

            public int getAge() {
                return age;
            }

            public void setAge(int age) {
                this.age = age;
            }
        }

}
