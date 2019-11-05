package com.cn.nrdp.sparksqlapi;

import com.cn.nrdp.main.PropertieUtils;
import org.apache.commons.lang3.JavaVersion;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.omg.CORBA.StringHolder;
import scala.Function1;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.JavaConversions;
import static org.apache.spark.sql.functions.col;

import java.util.*;

/**
 * 〈一句话功能简述〉<br>
 * 〈〉
 *
 * @author yujiang
 * @create 2019/1/2
 * @since 1.0.0
 */

public class readJsonDataframe {
    public static void main(String[] args) throws AnalysisException {
        SparkConf sparkConf = new SparkConf();
        Set<Tuple2<String, String>> initsparkconf = PropertieUtils.getPropertiesToList("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/initsparkconf");
        sparkConf.setAll(JavaConversions.asScalaSet(initsparkconf));
        //1.创建 入口 sparksession
        SparkSession sparkSession = SparkSession
                     .builder()
                     .master("local[2]")
                     .appName("sql test")
                     .config(sparkConf)
                     .getOrCreate();





//           runBasicDataFrameExample(sparkSession);
//           runDatasetCreationExample(sparkSession);
//           runInferSchemaExample(sparkSession);
             runProgrammaticSchema(sparkSession);
    }

    public static void runBasicDataFrameExample(SparkSession sparkSession) throws AnalysisException {
        //从已有的json schema中获取DataFrame
        Dataset<Row> dataset = sparkSession.read().json("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.json");
        //展示所有
        dataset.show();

        /**
         * +---+-------+
         * |age|   name|
         * +---+-------+
         * | 40|Michael|
         * | 30|   Andy|
         * | 19| Justin|
         * +---+-------+
         */

        dataset.printSchema();

        /**
         * root
         *  |-- age: long (nullable = true)
         *  |-- name: string (nullable = true)
         */

        //展示单列
        dataset.select("name").show();

        /**
         * +-------+
         * |   name|
         * +-------+
         * |Michael|
         * |   Andy|
         * | Justin|
         * +-------+
         */

//        此处关于TypedColumn 有疑问
//        TypedColumn column = new TypedColumn(, )


        //展示双列，并在age列做运算
        dataset.select(col("name"),col("age").plus(1)).show();
        /**
         * +-------+---------+
         * |   name|(age + 1)|
         * +-------+---------+
         * |Michael|       41|
         * |   Andy|       31|
         * | Justin|       20|
         *
         */

        //过滤大于21的
        dataset.filter(col("age").gt(21)).show();

        /**
         * +---+-------+
         * |age|   name|
         * +---+-------+
         * | 40|Michael|
         * | 30|   Andy|
         * +---+-------+
         */

        //分组聚合  并 求出分组后每组的个数
        dataset.groupBy("age").count().show();
        /**
         * +---+-----+
         * |age|count|
         * +---+-----+
         * | 40|    1|
         * | 30|    1|
         * | 19|    1|
         * +---+-----+
         */

        //将datafrume做成一个sql的临时表
        dataset.createOrReplaceTempView("people");


        //从临时表中查询数据
        sparkSession.sql("select * from people").show();

        sparkSession.sql("select * from people where age > '21' " ).show();


        //将dataFrume 做成一个sql全局临时表，全局临时表会保存在系统库 `global_temp`
        dataset.createGlobalTempView("people1");

        sparkSession.sql("select * from global_temp.people1 where name = 'Andy' ").show();

        //全局临时表示一个交叉会话
        sparkSession.newSession().sql("select * from global_temp.people1").show();
    }

    private static void runDatasetCreationExample(SparkSession sparkSession){
        /**
         * 将javabean作为创建dataframe的源
         */
        PerpeoJasonBean perpeoJasonBean = new PerpeoJasonBean();
        perpeoJasonBean.setName("Andy");
        perpeoJasonBean.setAge(20);

        Encoder<PerpeoJasonBean> bean = Encoders.bean(PerpeoJasonBean.class);

        Dataset<PerpeoJasonBean> dataset = sparkSession.createDataset(Collections.singletonList(perpeoJasonBean), bean);
        dataset.createOrReplaceTempView("youngman");
        Dataset<Row> sql = sparkSession.sql("select * from youngman");
        sql.show();
//        dataset.show();


        /**
         * 将数组list作为创建dataframe的源
         */
        Encoder<Integer> anInt = Encoders.INT();
        Dataset<Integer> sparkSessionDataset = sparkSession.createDataset(Arrays.asList(1, 2, 3), anInt);
        Dataset<Integer> map = sparkSessionDataset.map(new MapFunction<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                return integer+1;
            }
        }, anInt);
        map.groupBy("value").count().show();



        //通过bean类可以反射出datafreame
        Dataset<PerpeoJasonBean> as = sparkSession.read().json("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.json").as(bean);
        as.show();


    }

    public static void  runInferSchemaExample(SparkSession sparkSession){
        JavaRDD<PerpeoJasonBean> beanJavaRDD = sparkSession.read().textFile("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.txt")
                .javaRDD()
                .map(new Function<String, PerpeoJasonBean>() {
                    PerpeoJasonBean perpeoJasonBean = new PerpeoJasonBean();

                    public PerpeoJasonBean call(String s) throws Exception {
                        String[] split = s.split(",");
                        perpeoJasonBean.setName(split[0]);
                        Integer integer = Integer.parseInt(split[1].trim());
                        perpeoJasonBean.setAge(integer);
                        return perpeoJasonBean;
                    }
                });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(beanJavaRDD,PerpeoJasonBean.class);

        dataFrame.createOrReplaceTempView("people");

        Dataset<Row> sql = sparkSession.sql("select name from people where age > 19 and age < 30");

        sql.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name : "+row.getString(0);
            }
        }, Encoders.STRING()).show();


    }



    public static void runProgrammaticSchema(SparkSession sparkSession){
        RDD<String> stringRDD = sparkSession.sparkContext().textFile("/Users/yujiang/IdeaProjects/sparksql/src/main/resources/people.txt", 1);
        JavaRDD<String> stringJavaRDD = stringRDD.toJavaRDD();
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")){
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType structType = DataTypes.createStructType(fields);
        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(new Function<String, Row>() {
            public Row call(String v1) throws Exception {
                String[] attributes = v1.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, structType);
        dataFrame.createOrReplaceTempView("people");
        Dataset<Row> result = sparkSession.sql("select * from people");
        Dataset<String> namesDS = result.map(new MapFunction<Row, String>() {
            public String call(Row value) throws Exception {
                return "Name : "+value.getString(0);
            }
        }, Encoders.STRING());
        namesDS.show();

    }


    public static class PerpeoJasonBean implements Serializable{
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "PerpeoJasonBean{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
