/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wyb.spark.lineage.linkis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import com.wyb.spark.lineage.linkis.{SparkHiveObject, SparkSQLHistoryParser}
import org.apache.spark.sql.SQLContext

/**
 * Description:
 */
object SparkLogicalPlanHelper extends Logging{

  def extract(context: SQLContext, command: String, queryExecution: QueryExecution, startTime: Long): java.util.List[SparkHiveObject] = {
    if (queryExecution == null) return null
    val logicPlan = queryExecution.analyzed
    val (in, out) = SparkSQLHistoryParser.parse(logicPlan)
    in
  }

}
