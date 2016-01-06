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
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, SortOrder, Attribute}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

case class AssembleToRowBatch(child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputRowBatches: Boolean = true
  override def canProcessRowBatches: Boolean = false
  override def canProcessRows: Boolean = true
  override def doBatchExecute(): RDD[RowBatch] = {
    child.execute().mapPartitionsInternal { iter =>
      val schema = child.output.map(_.dataType)
      val rb = RowBatch.create(schema)
      val updaters = rb.updaters()
      val rbCapacity = rb.capacity

      val specificInserter = GenerateRowInserter.generate(schema)

      new Iterator[RowBatch] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): RowBatch = {
          rb.reset()
          var rowCount: Int = 0
          while (iter.hasNext && rowCount < rbCapacity) {
            specificInserter.insert(iter.next(), updaters)
            rowCount += 1
          }
          rb.size = rowCount
          if (rowCount < rbCapacity) {
            rb.endOfFile = true
          }
          rb
        }
      }
    }
  }

  override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)
}

case class DissembleFromRowBatch(child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputRowBatches: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessRows: Boolean = false
  override def doExecute(): RDD[InternalRow] = {
    child.batchExecute().mapPartitionsInternal { iter =>
      val schema = child.output.map(_.dataType)
      val row = new GenericMutableRow(schema.size)

      val specificGetter = GenerateRowGetter.generate(schema)

      new Iterator[InternalRow] {
        var currentRowBatch: RowBatch = null
        var rowIdxInCurrentBatch: Int = 0

        override def hasNext: Boolean = {
          if (iter.hasNext) {
            true
          } else if (currentRowBatch != null && rowIdxInCurrentBatch < currentRowBatch.size) {
            true
          } else {
            false
          }
        }

        override def next(): InternalRow = {
          if (currentRowBatch == null || rowIdxInCurrentBatch == currentRowBatch.size) {
            currentRowBatch = iter.next()
            rowIdxInCurrentBatch = 0
          }
          specificGetter.get(row, currentRowBatch.columns, rowIdxInCurrentBatch)
          rowIdxInCurrentBatch += 1
          row
        }
      }
    }
  }
}
