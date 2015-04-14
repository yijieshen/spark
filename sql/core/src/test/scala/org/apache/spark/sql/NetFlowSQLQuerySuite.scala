/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.BeforeAndAfterAll

class NetFlowSQLQuerySuite extends QueryTest with BeforeAndAfterAll {

  import org.apache.spark.sql.test.TestSQLContext.implicits._
  val sqlCtx = TestSQLContext

  test("eliminate partition filters in execution.Filter") {
    setConf("spark.sql.parquet.filterPushdown", "true")
    setConf("spark.sql.parquet.task.side.metadata", "false")

    val df = parquetFile("file:///Users/yijie/parquets3")

    val result = df
      .filter($"flow_time" > 1420041600)
      .filter($"flow_time" < 1451491200)
      .filter($"ipv4_dst_addr" === Array[Byte](78, 88, 120, 63))
      .filter($"dt" >= "2015-01-01")
      .filter($"dt" <= "2015-01-02")
      .filter($"hr" < 20)
      .select("ipv4_dst_addr")
      .groupBy("ipv4_dst_addr")
      .agg("ipv4_dst_addr" -> "count")

    result.show()
  }

}
