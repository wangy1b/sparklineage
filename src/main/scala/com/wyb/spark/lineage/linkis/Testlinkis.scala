package com.wyb.spark.lineage.linkis

import org.apache.spark.sql.SparkSession

object Testlinkis {
  def test2: Unit = {
    val spark = SparkSession.builder()
        .enableHiveSupport()
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .master("local[*]")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        // 指定自定义血缘提取插件
        .getOrCreate()

    spark.listenerManager.register(new MyQueryExLtr2)
    //spark.sql("""create table t1 as select 1 as id ,"dix" as name """)
    //spark.sql("""create table t2(id int,name string) partitioned by (dt string)""")
    println("**********")
    spark.sql("""insert into t2 partition (dt='2021')  select id ,name from t1 """)


    // get lineage :
    // ([default.t1 : [CSColumn{name='id', type='integer'}, CSColumn{name='name', type='string'}]],[default.t2 : [CSColumn{name='id', type='integer'}, CSColumn{name='name', type='string'}, CSColumn{name='dt', type='string'}]])
    spark.stop()
  }


  def main(args: Array[String]): Unit = {
    test2
  }

}
