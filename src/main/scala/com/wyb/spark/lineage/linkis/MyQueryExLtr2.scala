package com.wyb.spark.lineage.linkis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MyQueryExLtr2 extends QueryExecutionListener with Logging {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logInfo("funcName: "+funcName)
    val tuple2 = SparkSQLHistoryParser.parse(qe.analyzed)
    logInfo(tuple2.toString())
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}
