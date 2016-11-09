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

import scala.collection.mutable

import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{GenerateBatchCopier, InterBatchOrdering}
import org.apache.spark.sql.catalyst.vector.{IntArrayTimSort, IntComparator, RowBatch}

case class InMemoryBatchSorter(
    consumer: MemoryConsumer,
    interBatchOrdering: InterBatchOrdering,
    schema: Seq[Attribute],
    defaultCapacity: Int) {

  val sortedBatches: mutable.ArrayBuffer[RowBatch] = mutable.ArrayBuffer.empty[RowBatch]
  var all: Array[Int] = null
  var starts: Array[Int] = null
  var lengths: Array[Int] = null
  var batchCount: Int = 0
  var totalSize: Int = 0

  val taskMemoryManager = TaskContext.get().taskMemoryManager()
  val allocateGranularity: Long = 16 * 1024 * 1024; // 16 MB

  var allocated: Long = 0
  var firstTime: Boolean = true

  val comparator: IntComparator = new IntComparator {
    override def compare(i1: Int, i2: Int): Int = {
      val b1: Int = i1 >>> 16
      val l1: Int = i1 & 65535
      val b2: Int = i2 >>> 16
      val l2: Int = i2 & 65535
      interBatchOrdering.compare(sortedBatches(b1), l1, sortedBatches(b2), l2)
    }
  }

  def insertBatch(rb: RowBatch): Unit = {
    sortedBatches += rb
    batchCount += 1
    totalSize += rb.size

    if (allocated <= 0) {
      taskMemoryManager.acquireExecutionMemory(allocateGranularity, MemoryMode.OFF_HEAP, consumer)
      consumer.addUsed(allocateGranularity)
      allocated = allocateGranularity
    }

    val arraySize = (rb.size + 2) * 4 + (if (firstTime) {firstTime = false; 3 * 16} else 0)
    if (allocated > arraySize) {
      allocated -= arraySize
    } else {
      val short = arraySize - allocated
      taskMemoryManager.acquireExecutionMemory(allocateGranularity, MemoryMode.OFF_HEAP, consumer)
      consumer.addUsed(allocateGranularity)
      allocated = allocateGranularity - short
    }
  }

  def arrayFootprint(): Long = {
    if (sortedBatches.isEmpty) return 0
    val actualSize = 16 + totalSize * 4 + (16 + batchCount * 4) * 2
    val numAllocation = actualSize / allocateGranularity +
      (if (actualSize % allocateGranularity != 0) 1 else 0)
    numAllocation * allocateGranularity
  }

  def numBatches(): Int = sortedBatches.size

  def getMemoryUsage(): Long = {
    if (sortedBatches.isEmpty) return 0

    val arraySize = arrayFootprint()
    val batchSize = if (sortedBatches.nonEmpty) sortedBatches.head.memoryFootprintInBytes() else 0
    val numBatchPerAllocation: Long = allocateGranularity / batchSize
    var allocationCount: Long = sortedBatches.size / numBatchPerAllocation
    if (sortedBatches.size % numBatchPerAllocation != 0) {
      allocationCount += 1
    }

    allocationCount * allocateGranularity + arraySize
  }

  def freeMemory(): Unit = {
    batchCount = 0
    totalSize = 0
    firstTime = true
    allocated = 0
    all = null
    starts = null
    lengths = null

    var i = 0
    while (i < sortedBatches.size) {
      val rb = sortedBatches(i)
      rb.free()
      i += 1
    }
    sortedBatches.clear()
  }

  def preSort(): Unit = {
    all = new Array[Int](totalSize)
    starts = new Array[Int](batchCount)
    lengths = new Array[Int](batchCount)

    var pos = 0
    var rbIdx = 0
    while (rbIdx < batchCount) {
      val rb = sortedBatches(rbIdx)
      starts(rbIdx) = pos
      lengths(rbIdx) = rb.size

      val higherBits = rbIdx << 16
      var rowIdx = 0
      while (rowIdx < rb.size) {
        all(pos) = higherBits | (rb.sorted(rowIdx) & 65535)

        pos += 1
        rowIdx += 1
      }

      rbIdx += 1
    }
  }

  def sort(): Unit = {
    preSort()
    IntArrayTimSort.msort(all, 0, all.length, comparator, starts, lengths)
  }

  val batchCopier = GenerateBatchCopier.generate(schema, defaultCapacity)

  def getSortedIterator(): RowBatchSorterIterator = {
    sort()

    new RowBatchSorterIterator {

      val rb: RowBatch = RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity)
      var i: Int = 0
      val total: Int = all.length

      override def hasNext(): Boolean = i < total

      @throws[IOException]
      override def loadNext(): Unit = {
        rb.reset()
        var rowIdx: Int = 0

        while (rowIdx < defaultCapacity && i < total) {
          val rbAndRow = all(i)
          val batch = rbAndRow >>> 16
          val line = rbAndRow & 65535

          batchCopier.copy(sortedBatches(batch), line, rb, rowIdx)

          i += 1
          rowIdx += 1
        }
      }

      override def currentBatch: RowBatch = rb

      override def clone(): AnyRef = {
        new RowBatchSorterIterator {

          val newRB: RowBatch =
            RowBatch.create(schema.map(_.dataType).toArray, defaultCapacity, MemoryMode.ON_HEAP)
          var j: Int = i

          override def hasNext(): Boolean = j < total

          @throws[IOException]
          override def loadNext(): Unit = {
            newRB.reset()
            var rowIdx: Int = 0

            while (rowIdx < defaultCapacity && j < total) {
              val rbAndRow = all(j)
              val batch = rbAndRow >>> 16
              val line = rbAndRow & 65535

              batchCopier.copy(sortedBatches(batch), line, newRB, rowIdx)

              j += 1
              rowIdx += 1
            }
          }

          override def currentBatch: RowBatch = newRB
        }
      }
    }
  }
}
