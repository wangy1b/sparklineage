package com.wyb.spark.lineage.reference

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import com.wyb.spark.lineage.reference.reporter.Reporter
import org.apache.spark.internal.Logging

case class SparkSqlLineageListener(reporters: List[Reporter], async: Boolean = true) extends QueryExecutionListener
    with Logging{
  private lazy val processor = new ReportProcessor(reporters)

  def this() = this(Config.createInstancesOf("reporter"))

  if (async) {
    processor.runInBackground()
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    logDebug(s"Logical plan:\n${qe.logical}")
    logInfo("Offering query execution to report processor")
    processor.offer(qe, async)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
  }
}
