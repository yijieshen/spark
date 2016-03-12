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

package org.apache.spark.sql.execution.vector

import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.execution._

case class BatchExchange(
    var newPartitioning: Partitioning,
    child: SparkPlan,
    @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  override def nodeName: String = {
    val extraInfo = coordinator match {
      case Some(exchangeCoordinator) if exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case Some(exchangeCoordinator) if !exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case None => ""
    }

    val simpleNodeName = if (tungstenMode) "BatchExchange" else "Exchange"
    s"$simpleNodeName$extraInfo"
  }

  /**
   * Returns true iff we can support the data type, and we are not doing range partitioning.
   */
  private lazy val tungstenMode: Boolean = !newPartitioning.isInstanceOf[RangePartitioning]


  override def outputPartitioning: Partitioning = newPartitioning

  override def output: Seq[Attribute] = child.output

  override def outputsUnsafeRows: Boolean = false
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  override def canProcessRowBatches: Boolean = true
  override def outputsRowBatches: Boolean = true

  @transient private lazy val sparkConf = child.sqlContext.sparkContext.getConf

  private val serializer: Serializer = {
    if (tungstenMode) {
      new UnsafeRowSerializer(child.output.size)
    } else {
      new SparkSqlSerializer(sparkConf)
    }
  }
}
