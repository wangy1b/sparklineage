package com.wyb.spark.lineage.test


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class SparkSqlLineageExtension(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {

    case _ =>
      val linege = new SparkLineageUtil()

      println(linege.resolveLogicPlan(plan, "default"))
      plan
  }
}
