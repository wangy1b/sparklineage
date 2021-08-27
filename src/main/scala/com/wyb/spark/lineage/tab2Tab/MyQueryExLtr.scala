package com.wyb.spark.lineage.tab2Tab

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MyQueryExLtr extends QueryExecutionListener with Logging {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logInfo("funcName: "+funcName)
    val linege = new SparkLineageUtil()
    val tuple2 = linege.resolveLogicPlan(qe.analyzed, "default")
    logInfo(tuple2.toString())
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}
