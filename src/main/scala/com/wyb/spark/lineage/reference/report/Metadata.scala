package com.wyb.spark.lineage.reference.report

case class Metadata(appName: String) {
  def toMap(): Map[String, String] = Map(
    "appName" -> appName
  )
}
