package com.wyb.spark.lineage.test

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MyQueryExecutionListener(funcName: String, qe: QueryExecution, ex: Exception) extends  QueryExecutionListener{
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println(qe.logical.toJSON)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println(qe.logical.toJSON)
  }
}