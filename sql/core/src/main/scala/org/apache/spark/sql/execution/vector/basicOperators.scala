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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchPredicate, BatchProjection}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{UnaryNode, SparkPlan}

case class BatchProject(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  override def outputsRowBatches: Boolean = true
  override def outputsUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  protected override def doExecute(): RDD[InternalRow] =
    // throw new UnsupportedOperationException(getClass.getName)
    doBatchExecute().mapPartitions { iter =>
      iter.map(_.rowIterator().asScala).flatten
    }

  protected override def doBatchExecute(): RDD[RowBatch] = {
    val numRows = longMetric("numRows")
    child.batchExecute().mapPartitionsInternal { iter =>
      val project = BatchProjection.create(projectList, child.output,
        subexpressionEliminationEnabled)
      iter.map { rowBatch =>
        numRows += rowBatch.size
        project(rowBatch)
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}


case class BatchFilter(condition: Expression, child: SparkPlan) extends UnaryNode {

  private[sql] override lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputsRowBatches: Boolean = true
  override def outputsUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)

  protected override def doBatchExecute(): RDD[RowBatch] = attachTree(this, "batchExecute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.batchExecute().mapPartitionsInternal { iter =>
      val predicate = GenerateBatchPredicate.generate(condition, child.output)
      iter.map { rowBatch =>
        numInputRows += rowBatch.size
        predicate.eval(rowBatch)
        numOutputRows += rowBatch.size
        rowBatch
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}
