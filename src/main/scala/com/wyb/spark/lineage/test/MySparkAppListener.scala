package com.wyb.spark.lineage.test

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class MySparkAppListener extends SparkListener with Logging {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId

    logInfo("************************ appId ******************" + appId.get)
    logInfo("************************ appName ******************" + applicationStart.appName)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logInfo("************************ app end time ************************ " + applicationEnd.time)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logInfo("************************ jobStart ************************ " + jobStart.stageInfos)

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logInfo("************************ jobEnd ************************ " + jobEnd.jobResult)


  }
}