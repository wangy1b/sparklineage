package com.wyb.spark.lineage.test

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import scala.collection.JavaConverters._

/*

可以得出如下的转换对应关系：

scala.collection.Iterable <=> Java.lang.Iterable
scala.collection.Iterable <=> java.util.Collection
scala.collection.Iterator <=> java.util.{ Iterator, Enumeration }
scala.collection.mutable.Buffer <=> java.util.List
scala.collection.mutable.Set <=> java.util.Set
scala.collection.mutable.Map <=> java.util.{ Map, Dictionary }
scala.collection.mutable.ConcurrentMap <=> java.util.concurrent.ConcurrentMap

scala.collection.Seq         => java.util.List
scala.collection.mutable.Seq => java.util.List
scala.collection.Set         => java.util.Set
scala.collection.Map         => java.util.Map
java.util.Properties         => scala.collection.mutable.Map[String, String]

 */

class QueryExecutionPaser extends Logging{

  def resolveLogicPlan(plan: LogicalPlan, currentDB: String): (util.Set[DcTable], util.Set[DcTable]) = {
    val inputTables = new util.HashSet[DcTable]()
    val outputTables = new util.HashSet[DcTable]()
    resolveLogic(plan, currentDB, inputTables, outputTables)
    (inputTables, outputTables)
  }

  def resolveLogic(plan: LogicalPlan, currentDB: String, inputTables: util.Set[DcTable], outputTables: util.Set[DcTable]): Unit = {
    plan match {

      case plan: Project =>
        val project = plan.asInstanceOf[Project]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Union =>
        val project = plan.asInstanceOf[Union]
        //继续解析其子类型
        for (child <- project.children) {
          resolveLogic(child, currentDB, inputTables, outputTables)
        }

      case plan: Join =>
        val project = plan.asInstanceOf[Join]
        //继续解析其子类型
        resolveLogic(project.left, currentDB, inputTables, outputTables)
        resolveLogic(project.right, currentDB, inputTables, outputTables)

      case plan: Aggregate =>
        val project = plan.asInstanceOf[Aggregate]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Filter =>
        val project = plan.asInstanceOf[Filter]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Generate =>
        val project = plan.asInstanceOf[Generate]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: RepartitionByExpression =>
        val project = plan.asInstanceOf[RepartitionByExpression]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SerializeFromObject =>
        val project = plan.asInstanceOf[SerializeFromObject]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapPartitions =>
        val project = plan.asInstanceOf[MapPartitions]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: DeserializeToObject =>
        val project = plan.asInstanceOf[DeserializeToObject]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Repartition =>
        val project = plan.asInstanceOf[Repartition]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Deduplicate =>
        val project = plan.asInstanceOf[Deduplicate]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Window =>
        val project = plan.asInstanceOf[Window]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: MapElements =>
        val project = plan.asInstanceOf[MapElements]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: TypedFilter =>
        val project = plan.asInstanceOf[TypedFilter]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: Distinct =>
        val project = plan.asInstanceOf[Distinct]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: GlobalLimit =>
        val project = plan.asInstanceOf[GlobalLimit]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: LocalLimit =>
        val project = plan.asInstanceOf[LocalLimit]
        //继续解析其子类型
        resolveLogic(project.child, currentDB, inputTables, outputTables)

      case plan: SubqueryAlias =>
        val project = plan.asInstanceOf[SubqueryAlias]
        val childInputTables = new util.HashSet[DcTable]()
        val childOutputTables = new util.HashSet[DcTable]()
        //继续解析其子类型
        resolveLogic(project.child, currentDB, childInputTables, childOutputTables)
        if (childInputTables.size() > 0) {
          inputTables.addAll(childInputTables)
        } else {
          val ss: Seq[Tuple2[String, String]] = project.schema.map(x => Tuple2(x.name, x.dataType.toString))
          val fieldTypes: util.List[Tuple2[String, String]] = new util.ArrayList[Tuple2[String, String]]()
          for(s <- ss)
            fieldTypes.add(s)
          inputTables.add(DcTable(currentDB, project.alias, fieldTypes))
        }

      case plan: HiveTableRelation =>
        val project = plan.asInstanceOf[HiveTableRelation]
        val identifier = project.tableMeta.identifier
        if (inputTables.size() == 0) {
          val ss: Seq[Tuple2[String, String]] = project.dataCols.map(x => Tuple2(x.name, x.dataType.toString))
          val fieldTypes = new util.ArrayList[Tuple2[String, String]]()
          for (s <- ss)
            fieldTypes.add(s)
          //解析出的表名作为输入表
          val dcTable = DcTable(identifier.database.getOrElse(currentDB), identifier.table, fieldTypes)
          inputTables.add(dcTable)
        }

      case plan: UnresolvedRelation =>
        val project = plan.asInstanceOf[UnresolvedRelation]
        if (inputTables.size() == 0) {
          val ss: Seq[Tuple2[String, String]] = project.schema.map(x => Tuple2(x.name, x.dataType.toString))
          val fieldTypes = new util.ArrayList[Tuple2[String, String]]()
          for (s <- ss)
            fieldTypes.add(s)
          //解析出的表名作为输入表
          val dcTable = DcTable(project.tableIdentifier.database.getOrElse(currentDB), project.tableIdentifier.table, fieldTypes)
          inputTables.add(dcTable)
        }

      case plan: InsertIntoTable =>
        val project = plan.asInstanceOf[InsertIntoTable]
        //解析出的表名为输出表
        resolveLogic(project.table, currentDB, outputTables, inputTables)
        resolveLogic(project.query, currentDB, inputTables, outputTables)
      // todo test InsertIntoHiveTable 20210827
      case plan: InsertIntoHiveTable =>
        val project = plan.asInstanceOf[InsertIntoHiveTable]
        resolveLogic(project.query, currentDB, inputTables, outputTables)
        if (outputTables.size() ==  0) {
          val fieldTypes = new util.ArrayList[Tuple2[String, String]]()
          val ss: Seq[Tuple2[String, String]] = project.outputColumns.map(x => Tuple2(x.name, x.dataType.toString))
          for (s <- ss)
            fieldTypes.add(s)
          //解析出的表名为输出表
          val dcTable = DcTable(project.table.identifier.database.getOrElse(currentDB), project.table.identifier.table, fieldTypes)
          outputTables.add(dcTable)
        }


      case plan: CreateTable =>
        val project = plan.asInstanceOf[CreateTable]
        if (project.query.isDefined) {
          resolveLogic(project.query.get, currentDB, inputTables, outputTables)
        }
        val tableIdentifier = project.tableDesc.identifier
        if (inputTables.size() == 0) {
          val ss: Seq[Tuple2[String, String]] = project.output.map(x => Tuple2(x.name, x.dataType.toString))
          val fieldTypes = new util.ArrayList[Tuple2[String, String]]()
          for (s <- ss)
            fieldTypes.add(s)

        //解析出的表名为输出表
        val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table,fieldTypes)
        outputTables.add(dcTable)
        }

      case plan: LogicalRDD =>
      //通过textFile读取文件得到rdd，再对rdd进行变换，最后将rdd注册成dataframe，这里对df的logicplan进行解析会得到LogicRDD，
      // 对于这种情况的解决思路是在调用textFile时记录产生的rdd，解析df的logicplan时获取其rdd，
      // 判断之前产生的rdd是否为当前rdd的祖先，如果是，则将之前rdd对应的表名计入。
      //        val project = plan.asInstanceOf[LogicalRDD]
      //        try {
      //          for (rdd <- rddTableMap.keySet()) {
      //            if (checkRddRelationShip(rdd, project.rdd)) {
      //              val tableName = rddTableMap.get(rdd)
      //              val db = StringUtils.substringBefore(tableName, ".")
      //              val table = StringUtils.substringAfter(tableName, ".")
      //              inputTables.add(DcTable(db, table))
      //            }
      //          }
      //        } catch {
      //          case e: Throwable => log.error("resolve LogicalRDD error:", e)
      //        }

      case _ => log.info("没有匹配到plan" + plan)
    }
  }

  case class DcTable(dbName: String, tbName: String, colums: util.List[Tuple2[String,String]]) {
    override def toString: String = {
      dbName.toString + "." + tbName.toString + ":" + colums.toString
    }
  }

  def apply(plan: LogicalPlan, currentDB: String,colums: util.ArrayList[Tuple2[String,String]]): (util.Set[DcTable], util
  .Set[DcTable]) ={
    resolveLogicPlan(plan, currentDB)
  }
}
