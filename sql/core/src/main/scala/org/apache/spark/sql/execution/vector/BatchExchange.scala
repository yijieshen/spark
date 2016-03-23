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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExchangeCoordinator, SparkPlan, UnaryNode}

case class BatchExchange(
  var newPartitioning: Partitioning,
  child: SparkPlan,
  @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException

  override def output: Seq[Attribute] = child.output
}

object BatchExchange {
  def apply(newPartitioning: Partitioning, child: SparkPlan): BatchExchange = {
    BatchExchange(newPartitioning, child, coordinator = None: Option[ExchangeCoordinator])
  }
}
