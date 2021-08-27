package com.wyb.spark.lineage.LineageCapture
import org.json4s._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._

object JsonTest {

  def main(args: Array[String]): Unit = {
    val json = parse(
      """
		{
			"op": "subQueryAlias",
			"class": "class org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias",
			"alias": "u",
			"child": {
				"op": "subQueryAlias",
				"class": "class org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias",
				"alias": "user",
				"child": {
					"op": "logicalPlan",
					"class": "class org.apache.spark.sql.catalyst.catalog.HiveTableRelation"
				}
			}
		}
      """)


    val rs = for {
      JObject(child) <- json
      JField("alias", JString(alias)) <- child
    } yield alias
  println(rs)
    // List(u, user)
  }

}
