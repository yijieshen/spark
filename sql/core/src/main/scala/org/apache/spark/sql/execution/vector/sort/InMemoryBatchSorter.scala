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
package org.apache.spark.sql.execution.vector.sort

import java.io.IOException
import java.util.Comparator

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchCopier, InterBatchOrdering}
import org.apache.spark.sql.catalyst.vector.RowBatch

case class InMemoryBatchSorter(
    interBatchOrdering: InterBatchOrdering,
    sortedBatches: mutable.ArrayBuffer[RowBatch],
    schema: Seq[Attribute],
    defaultCapacity: Int) {

  val comparator: Comparator[RowBatch] = new Comparator[RowBatch] {
    override def compare(r1: RowBatch, r2: RowBatch): Int = {
      interBatchOrdering.compare(r1, r1.sorted(r1.rowIdx), r2, r2.sorted(r2.rowIdx))
    }
  }

  val priorityQueue = new java.util.PriorityQueue[RowBatch](comparator)

  val batchCopier = GenerateBatchCopier.generate(schema, defaultCapacity)

  def getSortedIterator(): RowBatchSorterIterator = {
    sortedBatches.foreach { rb =>
      rb.rowIdx = 0
      priorityQueue.add(rb)
    }

    new RowBatchSorterIterator {

      val rb: RowBatch = RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity)

      override def hasNext(): Boolean = !priorityQueue.isEmpty

      @throws[IOException]
      override def loadNext(): Unit = {
        rb.reset(false)
        var rowIdx: Int = 0

        while (rowIdx < defaultCapacity && !priorityQueue.isEmpty) {
          val nextLineBatch = priorityQueue.remove()

          batchCopier.copy(nextLineBatch, nextLineBatch.sorted(nextLineBatch.rowIdx), rb, rowIdx)

          nextLineBatch.rowIdx += 1
          if (nextLineBatch.rowIdx < nextLineBatch.size) {
            priorityQueue.add(nextLineBatch)
          }

          rowIdx += 1
        }
      }

      override def currentBatch: RowBatch = rb
    }
  }

  def free(): Unit = {
    priorityQueue.clear()
  }

  def reset(): Unit = {
    priorityQueue.clear()
  }

  // def getMemoryUsage(): Long = {
  //
  // }
}
