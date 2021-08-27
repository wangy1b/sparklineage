package com.wyb.spark.lineage.LineageCapture

import com.wyb.spark.lineage.LineageCapture.flow.{DataFlow, FlowToDot, GraphElement}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import com.wyb.spark.lineage.test.Main

import scala.util.Random

object TestLineageCapture {

  def getSpark(): SparkSession = {
    // only instantiate Spark once the HDFS cluster is up
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Test")
        .enableHiveSupport()
        //.config(
        //  "spark.sql.warehouse.dir",
        //  "target/test/spark-warehouse"
        //)
        .getOrCreate
  }


  def test1: Unit = {

    val listener: DataLineageQueryExecutionListener =
      new DataLineageQueryExecutionListener()

    val jslist = listener.jslist

    getSpark().listenerManager.register(
      listener
    )

    val characters: DataFrame = getSpark().read
        .format("json")
        .options(Map("multiline" -> "true"))
        .load("data/in/lineagecap/characters.json")

    //characters.show()

    val locations: DataFrame = getSpark().read
        .format("json")
        .options(Map("multiline" -> "true"))
        .load("data/in/lineagecap/locations.json")

    //locations.show()

    val df = characters
        .join(locations, characters("location.url") === locations("url"))
        .select(
          characters("id").alias("charId"),
          characters("name").alias("charName"),
          characters("location.url").alias("charLocation"),
          locations("id").as("locationId"),
          locations("name").alias("locationName"),
          locations("url").alias("locationUrl")
        )

    df.show()

    //df.createOrReplaceTempView("t1")
    //getSpark().sql("create table t11 as select * from t1")



    df.write.mode("overwrite").format("parquet").save("data/out/charactersWithLocation.parquet")

    val lastJslog = jslist.last
    println( lastJslog.length )

    val graphs: List[GraphElement] =
      DataFlow.transform(
        org.json4s.jackson.JsonMethods.parse(lastJslog)
      )

    val dots: String =
      FlowToDot.graphToDot(graphs)

    println(dots)
  }

  def test2: Unit = {

    val r = new Random(1000)

    def pseudoRandomString: String = {
      r.alphanumeric.take(6).mkString
    }

    //val inMemoryAppender: InMemoryAppender = new InMemoryAppender

    val listener: DataLineageQueryExecutionListener =
      new DataLineageQueryExecutionListener()

    val jslist = listener.jslist
    getSpark().listenerManager.register(
      listener
    )
    val spark = getSpark()

    Main.generateTab2(spark, spark.sparkContext)

    spark.sql("""create table if not exists test.res(name string, cnt int)""")


    val txt = s""" insert into table test.res
                 |select u.name,sum(s.cnt) as sum_cnt
                 |from test.user u
                 |join test.sales s
                 |on u.id = s.id
                 |group by name
    """.stripMargin
    spark.sql(txt)



    //import spark.implicits._
    //import functions._
    //
    //val user : DataFrame = spark.table("test.user")
    //val sales : DataFrame = spark.table("test.sales")
    //
    //val res : DataFrame = user.join(sales, user.col("id") === sales.col("id"))
    //    .groupBy("name")
    //    .agg(sum("cnt") as "cnt_sum")



    //val logs: List[String] = inMemoryAppender.getLogResult

    val lastJslog = jslist.last
    //println("lastJslog : " + lastJslog)

    val graphs: List[GraphElement] = DataFlow.transform(org.json4s.jackson.JsonMethods.parse(lastJslog))

    val dots: String = FlowToDot.graphToDot(graphs)
    println(dots)
  }

  def main(args: Array[String]): Unit = {
    test1
    //test2
  }

}
