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

package org.apache.spark.sql.vector

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

case class Lineitem(
  l_orderkey: Long, l_partkey: Int, l_suppkey: Int, l_linenumber: Int,
  l_quantity: Double, l_extendedprice: Double, l_discount: Double,
  l_tax: Double, l_returnflag: String, l_linestatus: String, l_shipdate: String,
  l_commitdate: String, l_receiptdate: String, l_shipinstruct: String,
  l_shipmode: String, l_comment: String)

class TPCHSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("Query 1") {
    withSQLConf(
      ("spark.sql.vectorize.enabled", "false"),
      ("spark.sql.vectorize.agg.enabled", "false")) {
      val lineitem = sqlContext.read.orc("hdfs://localhost:9000/tpch_orc/lineitem")
      lineitem.registerTempTable("lineitem")
      val q1 =
      """
        |select
        |  l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice),
        |  sum(l_extendedprice * (1 - l_discount)),
        |  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
        |  avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(1)
        |from
        |  lineitem
        |where
        |  l_shipdate<='1998-09-02'
        |group by l_returnflag, l_linestatus
        |order by l_returnflag, l_linestatus
      """.stripMargin

      val result = sql(q1)
      result.explain(true)
      result.show(false)
    }
  }
}
