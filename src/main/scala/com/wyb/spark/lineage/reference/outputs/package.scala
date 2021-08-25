package com.wyb.spark.lineage.reference

package object outputs {
  sealed trait Output {
    val typeName: String
    def toMap(): Map[String, Any]
  }

  case class FsOutput(path: String, format: String, typeName: String = "fs") extends Output {
    override def toMap: Map[String, Any] = Map(
      "type" -> typeName,
      "format" -> format,
      "path" -> path
    )
  }

  case class FieldsOutput(path: String, tableName: String, fileds: Map[String,String], typeName: String = "fs")
      extends Output {
    override def toMap(): Map[String, Any] = Map(
      "path" -> path,
      "typeName" -> typeName,
      "tableName" -> tableName,
      "fileds" -> fileds.toList
    )
  }

}

