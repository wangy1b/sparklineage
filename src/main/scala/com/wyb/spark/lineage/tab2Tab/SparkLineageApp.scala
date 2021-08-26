package com.wyb.spark.lineage.tab2Tab

import org.apache.spark.sql.SparkSession

object SparkLineageApp extends App {

  val spark = SparkSession.builder()
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // 指定自定义血缘提取插件
      .withExtensions {
        extensions => extensions.injectOptimizerRule(SparkSqlLineageExtension)
      }
      .getOrCreate()

  //spark.sql("""create table t1 as select 1 as id ,"dix" as name """)
  spark.sql("""create table t2(id int,name string) partitioned by (dt string)""")
  println("**********")
  spark.sql("""insert into t2 partition (dt='2021')  select id ,name from t1 """)

  spark.stop()
}