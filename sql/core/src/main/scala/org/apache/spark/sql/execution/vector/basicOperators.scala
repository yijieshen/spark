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
import org.apache.spark.sql.catalyst.expressions.vector.{BatchProjection, GenerateBatchPredicate}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{LeafNode, SparkPlan, UnaryNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.LongType

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

  private val defaultBatchCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  protected override def doBatchExecute(): RDD[RowBatch] = {
    val numRows = longMetric("numRows")
    child.batchExecute().mapPartitionsInternal { iter =>
      val project = BatchProjection.create(projectList, child.output,
        subexpressionEliminationEnabled, defaultBatchCapacity)
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

  private val defaultBatchCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  protected override def doBatchExecute(): RDD[RowBatch] = attachTree(this, "batchExecute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.batchExecute().mapPartitionsInternal { iter =>
      val predicate = GenerateBatchPredicate.generate(
        condition, child.output, defaultBatchCapacity)
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

case class BatchRange(
    start: Long,
    step: Long,
    numSlices: Int,
    numElements: BigInt,
    output: Seq[Attribute]) extends LeafNode {

  override def outputsRowBatches: Boolean = true
  override def outputsUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = false
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)

  private val defaultCapacity =
    sqlContext.sparkContext.getConf.
      getInt(RowBatch.SPARK_SQL_VECTORIZE_BATCH_CAPACITY, RowBatch.DEFAULT_CAPACITY)

  protected override def doBatchExecute(): RDD[RowBatch] = attachTree(this, "batchExecute") {
    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex((i, _) => {
        val partitionStart = (i * numElements) / numSlices * step + start
        val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
        def getSafeMargin(bi: BigInt): Long =
          if (bi.isValidLong) {
            bi.toLong
          } else if (bi > 0) {
            Long.MaxValue
          } else {
            Long.MinValue
          }
        val safePartitionStart = getSafeMargin(partitionStart)
        val safePartitionEnd = getSafeMargin(partitionEnd)

        val rb = RowBatch.create((LongType :: Nil).toArray, defaultCapacity)

//        new Iterator[InternalRow] {
//          private[this] var number: Long = safePartitionStart
//          private[this] var overflow: Boolean = false
//
//          override def hasNext =
//            if (!overflow) {
//              if (step > 0) {
//                number < safePartitionEnd
//              } else {
//                number > safePartitionEnd
//              }
//            } else false
//
//          override def next() = {
//            val ret = number
//            number += step
//            if (number < ret ^ step < 0) {
//              // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
//              // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
//              // back, we are pretty sure that we have an overflow.
//              overflow = true
//            }
//
//            unsafeRow.setLong(0, ret)
//            unsafeRow
//          }
//        }

        new Iterator[RowBatch] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false

          override def hasNext: Boolean =
            if (!overflow) {
              if (step > 0) {
                number < safePartitionEnd
              } else {
                number > safePartitionEnd
              }
            } else false

          override def next(): RowBatch = {
            rb.size = 0;

            if (step > 0) {
              var i = 0
              while (number < safePartitionEnd && i < rb.capacity && !overflow) {
                val ret = number
                rb.columns(0).getLongVector()(i) = number
                number += step
                if (number < ret ^ step < 0) {
                  overflow = true
                }
                i += 1
              }
              rb.size = i
              rb
            } else {
              var i = 0
              while (number > safePartitionEnd && i < rb.capacity && !overflow) {
                val ret = number
                rb.columns(0).getLongVector()(i) = number
                number += step
                if (number < ret ^ step < 0) {
                  overflow = true
                }
                i += 1
              }
              rb.size = i
              rb
            }
          }
        }
      })
  }
}
