package com.wyb.spark.lineage.LineageCapture

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.json4s.JsonAST._

import scala.collection.mutable.ArrayBuffer


class DataLineageQueryExecutionListener()
    extends QueryExecutionListener
    with Logging {
  private lazy val visitor = new DataLineageJson
  //val jslist: CopyOnWriteArrayList[String] = new CopyOnWriteArrayList[String]()
  val jslist = new ArrayBuffer[String]()

  // actions: https://spark.apache.org/docs/latest/rdd-programming-guide.html
  val lineageFunctions: List[String] = List(
    "collect",
    "command",
    "count",
    "first",
    "take",
    "takeSample",
    "takeOrdered",
    "save",
    "saveAsTextFile",
    "saveAsSequenceFile",
    "saveAsObjectFile",
    "countByKey",
    "foreach"
  )

  def onSuccess(
      functionName: String,
      qe: QueryExecution,
      duration: Long
  ): Unit = {
    val t0 = System.currentTimeMillis()
    if (lineageFunctions.contains(functionName)) {
      qe.executedPlan.collectLeaves()
      lazy val lineage: JValue = visitor.visit(qe.analyzed)
      lazy val result: JValue = JObject(
        JField("user", JString(qe.sparkSession.sparkContext.sparkUser)) ::
          JField("appName", JString(qe.sparkSession.sparkContext.appName)) ::
          JField(
            "appId",
            JString(qe.sparkSession.sparkContext.applicationId)
          ) ::
          JField(
            "appAttemptId",
            JString(qe.sparkSession.sparkContext.applicationAttemptId match {
              case Some(name) => name
              case _          => ""
            })
          ) ::
          JField("duration", JInt(duration)) ::
          JField("lineage", lineage) ::
          Nil
      )
      jslist.append(org.json4s.jackson.compactJson(result))
      logInfo(org.json4s.jackson.compactJson(result))
    } else {
      logWarning(s" function $functionName ignored")
    }
    val t1 = System.currentTimeMillis()
    logTrace(s"onSuccess duration: ${t1 - t0} ms")
  }
  def onFailure(
      functionName: String,
      qe: QueryExecution,
      ex: Exception
  ): Unit = {
    logError( s"Exception during function $functionName, ${ex.toString}")
    ex.printStackTrace()
  }
}
