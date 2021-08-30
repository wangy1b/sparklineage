package com.wyb.spark.lineage.test

import com.wyb.spark.lineage.reference.SparkSqlLineageListener
import com.wyb.spark.lineage.reference.reporter.InMemoryReporter
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main extends Logging {

  def generateTab(spark: SparkSession, sc: SparkContext): Unit = {
    import spark.implicits._

    val rdd1 : RDD[Row] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e")))
        .map(row => Row(row._1.toInt,row._2.toString))
    val structFields1 = Array(StructField("id",IntegerType,true),StructField("name",StringType,true))
    val structType1 = StructType(structFields1)
    val df1 = spark.createDataFrame(rdd1,structType1)
    df1.createOrReplaceTempView("user")

    val rdd2 : RDD[Row] = sc.parallelize(Array((1,100),(2,10),(3,20),(4,40),(5,500),(1,500),(3,200)))
        .map(row => Row(row._1.toInt,row._2.toInt))
    val structFields2 = Array(StructField("id",IntegerType,true),StructField("cnt",IntegerType,true))
    val structType2 = StructType(structFields2)
    val df2 = spark.createDataFrame(rdd2,structType2)
    df2.createOrReplaceTempView("sales")

    val user : DataFrame = spark.table("user")
    val sales : DataFrame = spark.table("sales")
  }
  def generateTab2(spark: SparkSession, sc: SparkContext): Unit = {

    val path = "./"
    val file1 = path + "data/in/user"
    val file2 = path + "data/in/sales"

    spark.sql("drop database if exists test cascade")
    spark.sql("create database test")

    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE test.user (
         | id int,
         | name STRING
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
         |LOCATION '${file1}'
         |""".stripMargin)

    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE test.sales (
         | id int,
         | cnt int
         |)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |STORED AS TEXTFILE
         |LOCATION '${file2}'
         |""".stripMargin)
  }


  def test1: Unit = {

    val reporter: InMemoryReporter = InMemoryReporter(compression = None)
    val listener: SparkSqlLineageListener = SparkSqlLineageListener(List(reporter), async = false)


    val spark : SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("lineage")
            .config("spark.ui.enabled", "false")
            //.config("spark.sql.queryExecutionListeners",
            //  "com.wyb.spark.lineage.reference.SparkSqlLineageListener")
            //.config("spark.sql.queryExecutionListeners","com.wyb.spark.lineage.test.MyQueryExecutionListener")
            //.config("spark.extraListeners","com.wyb.spark.lineage.MySparkAppListener")
            .enableHiveSupport()
            .getOrCreate()


    val sc : SparkContext = spark.sparkContext

    //sc.addSparkListener(new MySparkAppListener)




    import spark.implicits._
    generateTab2(spark, sc)

    spark.listenerManager.register(listener)


    logDebug("queryExecutionListeners : " + sc.getConf.get("spark.sql.queryExecutionListeners","null"))




    spark.sql("""create table if not exists test.res(name string, cnt int)""")

    val txt = s""" insert into table test.res
      |select u.name,sum(s.cnt) as sum_cnt
      |from test.user u
      |join test.sales s
      |on u.id = s.id
      |group by name
    """.stripMargin
    //spark.sql(txt)



    //val user : DataFrame = spark.table("test.user")
    //val sales : DataFrame = spark.table("test.sales")
    //
    //val res : DataFrame = user.join(sales, user.col("id") === sales.col("id"))
    //    .groupBy("name")
    //    .agg(sum("cnt") as "cnt_sum")




    //res.repartition(1)
    //  .write
    //  .mode("overwrite")
    //  .format("csv")
    //  .save("data/out/report")
  }


  def test2: Unit = {
    val spark = SparkSession.builder()
        .appName("test")
        .enableHiveSupport()
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .master("local[*]")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        // 指定自定义血缘提取插件
        .config(
          "spark.sql.warehouse.dir",
          "spark-warehouse"
        )
            //.config("ha","")
        .getOrCreate()

    spark.listenerManager.register(new MyQueryExecutionListener)
    //spark.sql("""create table t1 as select 1 as id ,"dix" as name """)
    //spark.sql("""create table t2(id int,name string) partitioned by (dt string)""")
    println("**********")
    //spark.sql("""insert into t2 partition (dt='2021')  select id ,name from t1 """)


    val txt = s"""|insert into table test.res
                 |select u.name,sum(s.cnt) as sum_cnt
                 |from test.user u
                 |join test.sales s
                 |on u.id = s.id
                 |group by name
                 |""".stripMargin
    spark.sql(txt)


    val t1 =
      s"""
         |create table test.t1 as
         |select a.* from test.res a
         |join test.user u
         |on u.name = a.name
       """.stripMargin
    spark.sql(t1)
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    //test1
    test2
  }

}
