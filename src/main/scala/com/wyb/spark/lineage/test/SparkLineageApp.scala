package com.wyb.spark.lineage.test

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

  spark.sql("""insert into t2 partition (dt='2021')  select id ,"dix" as name from t1 """)

  spark.stop()
}