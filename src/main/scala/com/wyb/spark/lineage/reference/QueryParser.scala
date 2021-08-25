package org.chojin.spark.lineage.reference

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.{CreateTable, InsertIntoHadoopFsRelationCommand, LogicalRelation}
import com.wyb.spark.lineage.reference.inputs.How.How
import com.wyb.spark.lineage.reference.inputs.{Field, HiveInput, How, Input}
import com.wyb.spark.lineage.reference.outputs.{FieldsOutput, FsOutput}
import com.wyb.spark.lineage.reference.report.{Metadata, Report}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

object QueryParser extends Logging {

  def findSource(attr: AttributeReference, plan: LogicalPlan, how: How): Seq[Input] = findSource(attr.toAttribute, plan, how)

  def findSource(attr: Attribute, plan: LogicalPlan, how: How): Seq[Input] = {
    plan.collect {
      //case u: Union => u.children.flatMap(child => child.output.flatMap(childAttr => findSource(childAttr, child, how)))
      case r: HiveTableRelation if r.outputSet.contains(attr) => Seq(HiveInput(r.tableMeta.qualifiedName, Set(Field(attr.name, how))))
      case r: LogicalRelation if r.outputSet.contains(attr) => Seq(HiveInput(r.catalogTable.get.qualifiedName, Set(Field(attr.name, how))))
      case j: Join => {
        val conds = j.condition.map { cond =>
          cond.collect {
            case ar: AttributeReference => j.children.flatMap({ child => findSource(ar, child, How.JOIN) })
            case al: Alias => al.collect {
              case ar: AttributeReference => j.children.flatMap({ child => findSource(ar, child, How.JOIN) })
            }.flatten
          }.flatten
        }
          .getOrElse(Seq())
          .filter({ input => input.fields.map({ f => f.how == How.JOIN }).reduce((a, b) => a || b) })

        j.children.flatMap({ child => findSource(attr, child, null) ++ conds })
      }
      case p: Project => {
        p.projectList.flatMap { proj =>
          proj.collect {
            case ar: AttributeReference if ar.toAttribute == attr => findSource(ar, p.child, How.PROJECTION)
            case al: Alias if al.name == attr.name => al.collect {
              case ar: AttributeReference => findSource(ar, p.child, How.PROJECTION)
            }.flatten
          }.flatten
        }
      }
      case f: Filter => f.condition.collect {
        case ar: AttributeReference => findSource(ar, f.child, How.FILTER)
        case _ => Seq()
      }.flatten
      case a: Aggregate => {
        val groupings = a.groupingExpressions.collect {
          case ar: AttributeReference => findSource(ar, a.child, How.GROUPBY)
          case al: Alias => al.collect {
            case ar: AttributeReference => findSource(ar, a.child, How.GROUPBY)
          }.flatten
        }.flatten

        val aggregates = a.aggregateExpressions.diff(a.groupingExpressions).collect {
          case ar: AttributeReference if ar.toAttribute == attr => findSource(ar, a.child, How.AGGREGATE) ++ groupings
          case al: Alias if al.name == attr.name => al.collect {
            case ar: AttributeReference => findSource(ar, a.child, How.AGGREGATE) ++ groupings
          }.flatten
        }.flatten

        aggregates ++ groupings
      }
    }.flatten
  }

  def parseQuery(qe: QueryExecution): Option[Report] = {
    qe.logical.collectFirst {
      case c: InsertIntoHadoopFsRelationCommand => {
        val output = FsOutput(c.outputPath.toString, c.fileFormat.toString)

        val sources = c.query match {
          case u: Union => u.children.flatMap(child => child.output.map(childAttr => childAttr -> findSource(childAttr, child, null)))
          case query => query.output.map {
            attr => attr -> findSource(attr, query, null)
          }
        }

        val fields: Seq[(String, List[Input])] = sources
          .map{ case (field, rawInputs) =>
            val inputs = rawInputs
              .groupBy { case HiveInput(name, _, _) => (HiveInput, name) }
              .map { case (key, value) =>
                key match {
                  case (HiveInput, table) =>
                    HiveInput(
                      table,
                      value
                        .map(_.fields.filter({ col => col.how != null }))
                        .reduce((a, b) => a ++ b))
                }
              }
            field.name -> inputs.toList
          }
          .groupBy { case (k, _) => k }
          .map { case (k, v) => k -> v.flatMap(_._2).toList }
          .toSeq

        logDebug(s"Fields: $fields")

        val metadata = Metadata(qe.sparkSession.sparkContext.appName)
        Report(metadata, output, Map(fields: _*))
      }
      case d: InsertIntoHiveTable => {
        val output = FieldsOutput(d.table.location.getPath,
          d.table.identifier.table,
          d.table.dataSchema.map(s => {
            (s.name.toString, s.dataType.typeName.toString)
          }).toMap
        )

        val metadata = Metadata(qe.sparkSession.sparkContext.appName)
        val fields: Seq[(String, List[Input])] = null
        Report(metadata, output, Map(fields: _*))
      }

      case d: CreateTable => {
        val output = FieldsOutput(d.tableDesc.database,
          d.tableDesc.identifier.table,
          d.tableDesc.dataSchema.map(s => {
            (s.name.toString, s.dataType.typeName.toString)
          }).toMap
        )

        val metadata = Metadata(qe.sparkSession.sparkContext.appName)
        val fields: Seq[(String, List[Input])] = null
        println("lllllll" + output.fileds.toList.toString())
        Report(metadata, output, Map(fields: _*))
      }
      case other =>
        logInfo(s"Unable to produce report for node ${other.getClass.getSimpleName}")
        null
    }
  }
}
