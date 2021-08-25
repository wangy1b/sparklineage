package com.wyb.spark.lineage.reference

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.apache.spark.sql.execution.QueryExecution
import com.wyb.spark.lineage.reference.reporter.Reporter
import org.apache.spark.internal.Logging
import org.chojin.spark.lineage.reference.QueryParser

class ReportProcessor(private val reporters: List[Reporter]) extends Logging{

  private val queue = new LinkedBlockingQueue[QueryExecution](1000)

  private lazy val thread = new Thread {
    override def run(): Unit = processReports()
  }

  def runInBackground(): Unit = {
    thread.setName(getClass.getName.concat("-process"))
    thread.setDaemon(true)

    Option(Thread.currentThread().getContextClassLoader) match {
      case Some(loader) if getClass.getClassLoader != loader => thread.setContextClassLoader(loader)
      case _ => logDebug("Context loader not set")
    }

    thread.start()
  }

  def offer(qe: QueryExecution, async: Boolean = true): Unit = {
    if (!queue.offer(qe, 10L, TimeUnit.SECONDS)) {
      logWarning("Unable to query execution to queue; dropping on the floor")
      logDebug(s"Skipped query plan:\n${qe.logical.treeString(verbose = true)}")
    }

    if (!async) {
      processReport()
    }
  }

  def processReport() = {
    if (reporters.isEmpty) {
      logWarning("No lineage reporters found")
    } else {
      reporters.foreach(reporter => logInfo(s"Using reporter $reporter"))
    }

    logDebug("Polling for report to process")
    Option(queue.poll(500L, TimeUnit.MILLISECONDS)).foreach({qe => {
      logInfo("Processing report")
      logDebug(s"Query execution: ${qe.executedPlan}")

      QueryParser.parseQuery(qe).foreach(report => {
        logDebug(s"Produced report: ${report.prettyPrint}")

        reporters.foreach(reporter => reporter.report(report))

        logInfo("Successfully processed report")
      })
    }})
  }

  def processReports(): Unit = {
    logInfo("Starting report processor thread")

    val reportersStr = reporters.map(reporter => s"  $reporter").mkString("\n")
    logInfo(s"Found ${reporters.length} reporters:\n$reportersStr")

    var running = true

    while(running) {
      try {
        processReport()
      } catch {
        case _: InterruptedException => {
          logInfo("Caught InterruptedException; exiting...")
          running = false
        }
        case e: Throwable => {
          logError("Caught exception while processing report", e)
        }
      }
    }
  }
}
