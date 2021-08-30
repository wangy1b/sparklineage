package com.wyb.spark.lineage.test

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
class MyQueryExecutionListener() extends  QueryExecutionListener
    with  Logging {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logInfo( "logicalPlan : "+ qe.logical.prettyJson)
    val paser = new QueryExecutionPaser

    val (in,out): Tuple2[util.Set[paser.DcTable],util.Set[paser.DcTable]]= paser.resolveLogicPlan(qe.analyzed, "default")
    println("in:" +in.toString)
    println("out:"+out.toString)

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
}