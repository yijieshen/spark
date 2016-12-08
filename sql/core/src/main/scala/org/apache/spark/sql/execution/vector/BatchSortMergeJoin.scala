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

import java.util.Arrays

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, BoundReference, Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.vector._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType

case class BatchSortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputsRowBatches: Boolean = true
  override def outputsUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  private val defaultBatchCapacity: Int = sqlContext.conf.vectorizedBatchCapacity

  override def output: Seq[Attribute] = left.output ++ right.output

  val leftKeyPositions = leftKeys.map(
    BindReferences.bindReference(_, left.output).asInstanceOf[BoundReference].ordinal)
  val rightKeyPositions = rightKeys.map(
    BindReferences.bindReference(_, right.output).asInstanceOf[BoundReference].ordinal)

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  val leftOrder = requiredOrders(leftKeys)
  val rightOrder = requiredOrders(rightKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = leftOrder :: rightOrder :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)

  protected override def doBatchExecute(): RDD[RowBatch] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    left.batchExecute().zipPartitions(right.batchExecute()) { (leftIter, rightIter) =>
      val streamedBatchComparator = GenerateBatchOrdering.generate(
        leftOrder, left.output, defaultBatchCapacity)
      val streamedInterBatchComparator = GenerateInterBatchOrdering.generate(
        leftOrder, left.output, defaultBatchCapacity)
      val bufferedBatchComparator = GenerateBatchOrdering.generate(
        rightOrder, right.output, defaultBatchCapacity)
      val bufferedInterBatchComparator = GenerateInterBatchOrdering.generate(
        rightOrder, right.output, defaultBatchCapacity)
      val interBatchComparator = GenerateInterBatchComparator.generate(
        leftOrder :: rightOrder :: Nil,
        left.output :: right.output :: Nil, defaultBatchCapacity)
      val batchJoinCopier = GenerateBatchJoinCopier.generate(
        left.output :: right.output :: Nil, defaultBatchCapacity)

      val streamedCopierToTmp = GenerateBatchCopier.generate(left.output, defaultBatchCapacity)
      val bufferedCopierToTmp = GenerateBatchCopier.generate(right.output, defaultBatchCapacity)

      val smjScanner = new SortMergeJoinScanner(
        RowBatchIterator.fromScala(leftIter),
        numLeftRows,
        RowBatchIterator.fromScala(rightIter),
        numRightRows,
        numOutputRows,
        streamedBatchComparator,
        streamedInterBatchComparator,
        streamedCopierToTmp,
        bufferedBatchComparator,
        bufferedInterBatchComparator,
        bufferedCopierToTmp,
        interBatchComparator,
        batchJoinCopier,
        leftKeyPositions,
        rightKeyPositions,
        left.output.map(_.dataType).toArray,
        right.output.map(_.dataType).toArray,
        output.map(_.dataType).toArray,
        defaultBatchCapacity
      )

      smjScanner.getJoinedIterator().toScala
    }
  }
}

private class SortMergeJoinScanner(
    streamedIter: RowBatchIterator,
    numStreamedRows: LongSQLMetric,
    bufferedIter: RowBatchIterator,
    numBufferedRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
    streamedComparator: BatchOrdering,
    streamedInterBatchComparator: InterBatchOrdering,
    streamedCopierToTmp: BatchCopier,
    bufferedComparator: BatchOrdering,
    bufferedInterBatchComparator: InterBatchOrdering,
    bufferedCopierToTmp: BatchCopier,
    interBatchComparator: InterBatchComparator,
    batchJoinCopier: BatchJoinCopier,
    keyPosInLeft: Seq[Int],
    keyPosInRight: Seq[Int],
    leftSchema: Array[DataType],
    rightSchema: Array[DataType],
    outputSchema: Array[DataType],
    defaultCapacity: Int) {

  private[this] var streamedBatch: RowBatch = null
  private[this] var bufferedBatch: RowBatch = null
  private[this] var streamedSize: Int = 0
  private[this] var bufferedSize: Int = 0
  private[this] var streamedRowIdx: Int = 0
  private[this] var bufferedRowIdx: Int = 0

  private[this] val streamedRuns: Array[Int] = new Array[Int](defaultCapacity)
  private[this] val bufferedRuns: Array[Int] = new Array[Int](defaultCapacity)

  def getJoinedIterator(): RowBatchIterator = new RowBatchIterator {
    var rb = RowBatch.create(outputSchema, defaultCapacity)
    var another = RowBatch.create(outputSchema, defaultCapacity)
    var tmpRB: RowBatch = null

    val tmpStreamed = RowBatch.create(leftSchema, defaultCapacity)
    val tmpBuffered = RowBatch.create(rightSchema, defaultCapacity)
    var numStreamedInTmp: Int = 0
    var numBufferedInTmp: Int = 0

    var rbIdx: Int = 0
    var anotherIdx: Int = 0
    // var tmpIdx: Int = 0

    var findMatches: Boolean = false
    var numStreamed: Int = 0
    var numBuffered: Int = 0
    var numOutput: Int = 0
    var resultSpanTwoBatches: Boolean = false
    var numRowsInFirstBatch: Int = 0
    var numRowsInSecondBatch: Int = 0

    private def joining(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy streamedRowIdx + i to rbIdx + outputIdx
        batchJoinCopier.copyLeft(streamedBatch, streamedRowIdx + i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy streamedRowIdx + i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(streamedBatch, streamedRowIdx + i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }

      // copy buffered part
      i = 0; j = 0; outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy bufferedRowIdx + j to rbIdx + outputIdx
        batchJoinCopier.copyRight(bufferedBatch, bufferedRowIdx + j, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy bufferedRowIdx + j to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(bufferedBatch, bufferedRowIdx + j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
    }

    private def joiningTmp(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy i to rbIdx + outputIdx
        batchJoinCopier.copyLeft(tmpStreamed, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(tmpStreamed, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }

      // copy buffered part
      i = 0; j = 0; outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy j to rbIdx + outputIdx
        batchJoinCopier.copyRight(tmpBuffered, j, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy j + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(tmpBuffered, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
    }

    private def joiningBufferedAllInTmp(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch && i < numStreamedInTmp) {
        // copy tmp(i) to rbIdx + outputIdx
        batchJoinCopier.copyLeft(tmpStreamed, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput && i < numStreamedInTmp) {
        // copy tmp(i) to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(tmpStreamed, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      i -= numStreamedInTmp
      while (outputIdx < numRowsInFirstBatch) {
        // copy i to rbIdx + outputIdx
        batchJoinCopier.copyLeft(streamedBatch, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(streamedBatch, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }

      // copy buffered part
      i = 0; j = 0; outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy j to rbIdx + outputIdx
        batchJoinCopier.copyRight(tmpBuffered, j, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy j + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(tmpBuffered, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
    }

    private def joiningStreamedAllInTmp(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy i to rbIdx + outputIdx
        batchJoinCopier.copyLeft(tmpStreamed, i, rb, rbIdx + outputIdx)
        i += 1
        if (i == numStreamed) {
          i = 0
          j += 1
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(tmpStreamed, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        i += 1
        if (i == numStreamed) {
          i = 0
          j += 1
        }
        outputIdx += 1
      }

      // copy buffered part
      i = 0; j = 0; outputIdx = 0
      while (outputIdx < numRowsInFirstBatch && j < numBufferedInTmp) {
        // copy tmp(j) to rbIdx + outputIdx
        batchJoinCopier.copyRight(tmpBuffered, j, rb, rbIdx + outputIdx)
        i += 1
        if (i == numStreamed) {
          j += 1
          i = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput && j < numBufferedInTmp) {
        // copy tmp(j) + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(tmpBuffered, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        i += 1
        if (i == numStreamed) {
          j += 1
          i = 0
        }
        outputIdx += 1
      }

      j -= numBufferedInTmp
      while (outputIdx < numRowsInFirstBatch) {
        // copy j to rbIdx + outputIdx
        batchJoinCopier.copyRight(bufferedBatch, j, rb, rbIdx + outputIdx)
        i += 1
        if (i == numStreamed) {
          j += 1
          i = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy j + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(bufferedBatch, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        i += 1
        if (i == numStreamed) {
          j += 1
          i = 0
        }
        outputIdx += 1
      }
    }

    private def joiningBothSpan(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch && i < numStreamedInTmp) {
        // copy tmp(i) to rbIdx + outputIdx
        batchJoinCopier.copyLeft(tmpStreamed, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput && i < numStreamedInTmp) {
        // copy tmp(i) to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(tmpStreamed, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      i -= numStreamedInTmp
      while (outputIdx < numRowsInFirstBatch) {
        // copy i to rbIdx + outputIdx
        batchJoinCopier.copyLeft(streamedBatch, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyLeft(streamedBatch, i, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }

      // copy buffered part
      val bufferedRun = bufferedRuns(bufferedRowIdx)
      i = 0; j = 0; outputIdx = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy tmp(j) to rbIdx + outputIdx
        batchJoinCopier.copyRight(tmpBuffered, j, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBufferedInTmp) {
          j = 0
          outputIdx += bufferedRun
        } else {
          outputIdx += 1
        }
      }
      while (outputIdx < numOutput) {
        // copy tmp(j) to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(tmpBuffered, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == numBufferedInTmp) {
          j = 0
          outputIdx += bufferedRun
        } else {
          outputIdx += 1
        }
      }

      outputIdx = 0 + numBufferedInTmp
      j = 0
      while (outputIdx < numRowsInFirstBatch) {
        // copy j to rbIdx + outputIdx
        batchJoinCopier.copyRight(bufferedBatch, j, rb, rbIdx + outputIdx)
        j += 1
        if (j == bufferedRun) {
          j = 0
          outputIdx += numBufferedInTmp
        } else {
          outputIdx += 1
        }
      }
      while (outputIdx < numOutput) {
        // copy j to anotherIdx + (outputIdx - numRowsInFirstBatch)
        batchJoinCopier.copyRight(bufferedBatch, j, another,
          anotherIdx + (outputIdx - numRowsInFirstBatch))
        j += 1
        if (j == bufferedRun) {
          j = 0
          outputIdx += numBufferedInTmp
        } else {
          outputIdx += 1
        }
      }
    }

    override def advanceNext(): Boolean = {
      if (resultSpanTwoBatches) {
        tmpRB = rb
        rb = another
        rbIdx = anotherIdx
        another = tmpRB
        another.reset()
        anotherIdx = 0
        resultSpanTwoBatches = false
      } else {
        rb.reset()
        rbIdx = 0
      }

      findMatches = findNextInnerJoinRows()
      while (rbIdx < rb.capacity && findMatches) {
        // copy rows into output
        numStreamed = streamedRuns(streamedRowIdx)
        numBuffered = bufferedRuns(bufferedRowIdx)
        numOutput = numStreamed * numBuffered

        resultSpanTwoBatches = numOutput > defaultCapacity - rbIdx
        numRowsInFirstBatch =
          if (!resultSpanTwoBatches) numOutput else defaultCapacity - rbIdx
        numRowsInSecondBatch =
          if (resultSpanTwoBatches) numOutput - numRowsInFirstBatch else 0

        // runs inside streamed and buffered
        if (numStreamed + streamedRowIdx < streamedSize &&
          numBuffered + bufferedRowIdx < bufferedSize) {

          joining()
          advancedStreamed()
          advancedBuffered()

        } else if (numStreamed + streamedRowIdx < streamedSize) { // buffered span

          joining()
          advancedBuffered()

        } else if (numBuffered + bufferedRowIdx < bufferedSize) { // streamed span

          joining()
          advancedStreamed()

        } else { // both span
          // copy last key to tmp
          tmpStreamed.reset()
          var i = 0
          while (i < numStreamed) {
            streamedCopierToTmp.copy(streamedBatch, streamedRowIdx + i, tmpStreamed, i)
            i += 1
          }
          tmpBuffered.reset()
          i = 0
          while (i < numBuffered) {
            bufferedCopierToTmp.copy(bufferedBatch, bufferedRowIdx + i, tmpBuffered, i)
            i += 1
          }
          numStreamedInTmp = tmpStreamed.size
          val moreStreamedRows = advancedStreamed()

          numBufferedInTmp = tmpBuffered.size
          val moreBufferedRows = advancedBuffered()

          val streamedKeyContinues = if (moreStreamedRows) {
            streamedInterBatchComparator.compare(tmpStreamed, 0, streamedBatch, 0) == 0
          } else false

          val bufferedKeyContinues = if (moreBufferedRows) {
            bufferedInterBatchComparator.compare(tmpBuffered, 0, bufferedBatch, 0) == 0
          } else false

          numStreamed =
            (if (streamedKeyContinues) streamedRuns(streamedRowIdx) else 0) + numStreamedInTmp
          numBuffered =
            (if (bufferedKeyContinues) bufferedRuns(bufferedRowIdx) else 0) + numBufferedInTmp
          numOutput = numStreamed * numBuffered

          resultSpanTwoBatches = numOutput > defaultCapacity - rbIdx
          numRowsInFirstBatch =
            if (!resultSpanTwoBatches) numOutput else defaultCapacity - rbIdx
          numRowsInSecondBatch =
            if (resultSpanTwoBatches) numOutput - numRowsInFirstBatch else 0

          if (!streamedKeyContinues && !bufferedKeyContinues) {
            joiningTmp()
          } else if (streamedKeyContinues && !bufferedKeyContinues) {
            joiningBufferedAllInTmp()
          } else if (!streamedKeyContinues && bufferedKeyContinues) {
            joiningStreamedAllInTmp()
          } else {
            joiningBothSpan()
          }
        }
        rbIdx += numRowsInFirstBatch
        anotherIdx += numRowsInSecondBatch

        findMatches = findNextInnerJoinRows()
      }
      numOutputRows += rbIdx
      rb.size = rbIdx
      if (rbIdx == 0) false else true
    }

    override def getBatch: RowBatch = rb
  }

  // read in the first batches to start comparison
  advancedStreamed()
  advancedBuffered()

  // this should be called each time we get a new batch,
  // and will populate the runs array denoting equal keys
  def identifyRuns(runs: Array[Int], rb: RowBatch, comparator: BatchOrdering): Unit = {
    Arrays.fill(runs, 0)
    comparator.reset(rb)

    var i: Int = 0
    var runStarted: Boolean = false
    var runCount: Int = 1
    var start: Int = 0
    runs(0) = 1

    val iteration: Int = rb.size - 1

    while (i < iteration) {
      val comp = comparator.compare(i, i + 1)

      if (comp == 0) {
        if (!runStarted) {
          runStarted = true
          runCount = 2
          start = i
        } else {
          runCount += 1
        }
        runs(i + 1) = 0
      } else if (comp < 0) {
        if (runStarted) {
          runs(start) = runCount
          runStarted = false
        }
        runs(i + 1) = 1
      } else {
        throw new RuntimeException(
          s"This should never happen since rows should be sorted already, wrong at $i")
      }

      i += 1
    }
    if (runStarted) {
      runs(start) = runCount
    }
  }

  def identifyRunsOnStreamedSide(): Unit =
    identifyRuns(streamedRuns, streamedBatch, streamedComparator)

  def identifyRunsOnBufferedSide(): Unit =
    identifyRuns(bufferedRuns, bufferedBatch, bufferedComparator)

  def findNextInnerJoinRows(): Boolean = {
    if (streamedBatch == null || bufferedBatch == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      false
    } else {
      var comp = 0
      do {
        comp = interBatchComparator.compare(
          streamedBatch, streamedRowIdx, bufferedBatch, bufferedRowIdx)
        if (comp > 0) advancedBuffered()
        else if (comp < 0) advancedStreamed()
      } while (streamedBatch != null && bufferedBatch != null && comp != 0)
      if (streamedBatch == null || bufferedBatch == null) {
        false
      } else {
        true
      }
    }
  }


  private def advancedStreamed(): Boolean = {
    var foundRow: Boolean = false
    while (!foundRow) {
      if (streamedBatch == null || streamedRowIdx >= streamedSize) {
        if (!streamedIter.advanceNext()) {
          streamedBatch = null
          streamedSize = 0
          streamedRowIdx = 0
          return false
        } else {
          streamedBatch = streamedIter.getBatch
          streamedSize = streamedBatch.size
          streamedRowIdx = 0
          identifyRunsOnStreamedSide()
        }
      } else {
        val keyRuns = streamedRuns(streamedRowIdx)
        streamedRowIdx += keyRuns
        numStreamedRows += keyRuns
      }
      if (streamedRowIdx >= streamedSize) {
        // do nothing and wait for next iteration to get a new batch
      } else if (anyNullInStreamedKey()) {
        val keyRuns = streamedRuns(streamedRowIdx)
        streamedRowIdx += keyRuns
        numStreamedRows += keyRuns
      } else {
        foundRow = true
      }
    }
    foundRow
  }

  private def advancedBuffered(): Boolean = {
    var foundRow: Boolean = false
    while (!foundRow) {
      if (bufferedBatch == null || bufferedRowIdx >= bufferedSize) {
        if (!bufferedIter.advanceNext()) {
          bufferedBatch = null
          bufferedSize = 0
          bufferedRowIdx = 0
          return false
        } else {
          bufferedBatch = bufferedIter.getBatch
          bufferedSize = bufferedBatch.size
          bufferedRowIdx = 0
          identifyRunsOnBufferedSide()
        }
      } else {
        val keyRuns = bufferedRuns(bufferedRowIdx)
        bufferedRowIdx += keyRuns
        numBufferedRows += keyRuns
      }
      if (bufferedRowIdx >= bufferedSize) {
        // do nothing and wait for next iteration to get a new batch
      } else if (anyNullInBufferedKey()) {
        val keyRuns = bufferedRuns(bufferedRowIdx)
        bufferedRowIdx += keyRuns
        numBufferedRows += keyRuns
      } else {
        foundRow = true
      }
    }
    foundRow
  }

  private def anyNullInStreamedKey(): Boolean = {
    !keyPosInLeft.forall(!streamedBatch.columns(_).isNullAt(streamedRowIdx))
  }

  private def anyNullInBufferedKey(): Boolean = {
    !keyPosInRight.forall(!bufferedBatch.columns(_).isNullAt(bufferedRowIdx))
  }

}
