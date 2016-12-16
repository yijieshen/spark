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

  protected def doBatchExecute1(): RDD[RowBatch] = {
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

      val originJoinCopier = GenerateBatchJoinCopier.generate(
        left.output :: right.output :: Nil, defaultBatchCapacity)
      val batchJoinCopier = GenerateBatchJoinCWCopier.generate(
        left.output :: right.output :: Nil, defaultBatchCapacity)

      val streamedCopierToTmp =
        GenerateBatchColumnWiseCopier.generate(left.output, defaultBatchCapacity)
      val bufferedCopierToTmp =
        GenerateBatchColumnWiseCopier.generate(right.output, defaultBatchCapacity)

      val smjScanner = new SortMergeJoinScanner2(
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
        originJoinCopier,
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

  def findNextOuterJoinRows(): Boolean = {
    if (streamedBatch == null) {
      false
    } else {
      var comp = 0
      do {
        comp = interBatchComparator.compare(
          streamedBatch, streamedRowIdx, bufferedBatch, bufferedRowIdx)
      } while (comp > 0 && advancedBuffered())
      true
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

// scalastyle:off
private class SortMergeJoinScanner2(
    streamedIter: RowBatchIterator,
    numStreamedRows: LongSQLMetric,
    bufferedIter: RowBatchIterator,
    numBufferedRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
    streamedComparator: BatchOrdering,
    streamedInterBatchComparator: InterBatchOrdering,
    streamedCopierToTmp: BatchColumnWiseCopier,
    bufferedComparator: BatchOrdering,
    bufferedInterBatchComparator: InterBatchOrdering,
    bufferedCopierToTmp: BatchColumnWiseCopier,
    interBatchComparator: InterBatchComparator,
    originJoinCopier: BatchJoinCopier,
    batchJoinCopier: BatchJoinCWCopier,
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

    var numRepeatInFirstBatch: Int = 0
    var incompleteRunInFirstBatch: Int = 0
    var incompleteRunInSecondBatch: Int = 0
    var numRepeatInSecondBatch: Int = 0

    lazy val streamedRunIsLonger: Boolean = identifyLongerRun()

    private def joiningBothSpan(): Unit = {
      // copy streamed part
      var i = 0; var j = 0; var outputIdx = 0
      while (outputIdx < numRowsInFirstBatch && i < numStreamedInTmp) {
        // copy tmp(i) to rbIdx + outputIdx
        originJoinCopier.copyLeft(tmpStreamed, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput && i < numStreamedInTmp) {
        // copy tmp(i) to anotherIdx + (outputIdx - numRowsInFirstBatch)
        originJoinCopier.copyLeft(tmpStreamed, i, another,
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
        originJoinCopier.copyLeft(streamedBatch, i, rb, rbIdx + outputIdx)
        j += 1
        if (j == numBuffered) {
          i += 1
          j = 0
        }
        outputIdx += 1
      }
      while (outputIdx < numOutput) {
        // copy i to anotherIdx + (outputIdx - numRowsInFirstBatch)
        originJoinCopier.copyLeft(streamedBatch, i, another,
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
        originJoinCopier.copyRight(tmpBuffered, j, rb, rbIdx + outputIdx)
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
        originJoinCopier.copyRight(tmpBuffered, j, another,
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
        originJoinCopier.copyRight(bufferedBatch, j, rb, rbIdx + outputIdx)
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
        originJoinCopier.copyRight(bufferedBatch, j, another,
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

    private def genOutputLocation(): Unit = {
      resultSpanTwoBatches = numOutput > defaultCapacity - rbIdx
      numRowsInFirstBatch =
        if (!resultSpanTwoBatches) numOutput else defaultCapacity - rbIdx
      numRowsInSecondBatch =
        if (resultSpanTwoBatches) numOutput - numRowsInFirstBatch else 0
    }

    private def genLeftRunStatistics(): Unit = {
      numRepeatInFirstBatch = numRowsInFirstBatch / numStreamed
      incompleteRunInFirstBatch = numRowsInFirstBatch % numStreamed
      incompleteRunInSecondBatch = if (incompleteRunInFirstBatch > 0) {
        numStreamed - incompleteRunInFirstBatch
      } else 0
      numRepeatInSecondBatch = numRowsInSecondBatch / numStreamed
    }

    private def genRightRunStatistics(): Unit = {
      numRepeatInFirstBatch = numRowsInFirstBatch / numBuffered
      incompleteRunInFirstBatch = numRowsInFirstBatch % numBuffered
      incompleteRunInSecondBatch = if (incompleteRunInFirstBatch > 0) {
        numBuffered - incompleteRunInFirstBatch
      } else 0
      numRepeatInSecondBatch = numRowsInSecondBatch / numBuffered
    }

    private def leftRunJoin(): Unit = {
      genLeftRunStatistics()

      batchJoinCopier.copyLeftRuns(streamedBatch, streamedRowIdx, rb, rbIdx, numRepeatInFirstBatch, numStreamed)
      var j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRun(streamedBatch, streamedRowIdx, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRun(streamedBatch, streamedRowIdx + incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRuns(streamedBatch, streamedRowIdx, another, j, numRepeatInSecondBatch, numStreamed)

      batchJoinCopier.copyRightRepeats(bufferedBatch, bufferedRowIdx, rb, rbIdx, numStreamed, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRepeat(bufferedBatch, bufferedRowIdx + i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRepeat(bufferedBatch, bufferedRowIdx + i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRepeats(bufferedBatch, bufferedRowIdx + i, another, j, numStreamed, numBuffered - i)
    }

    private def rightRunJoin(): Unit = {
      genRightRunStatistics()

      batchJoinCopier.copyRightRuns(bufferedBatch, bufferedRowIdx, rb, rbIdx, numRepeatInFirstBatch, numBuffered)
      var j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRun(bufferedBatch, bufferedRowIdx, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRun(bufferedBatch, bufferedRowIdx + incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRuns(bufferedBatch, bufferedRowIdx, another, j, numRepeatInSecondBatch, numBuffered)

      batchJoinCopier.copyLeftRepeats(streamedBatch, streamedRowIdx, rb, rbIdx, numBuffered, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRepeat(streamedBatch, streamedRowIdx + i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRepeat(streamedBatch, streamedRowIdx + i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRepeats(streamedBatch, streamedRowIdx + i, another, j, numBuffered, numStreamed - i)
    }

    private def leftRunJoinTmp(): Unit = {
      genLeftRunStatistics()

      batchJoinCopier.copyLeftRuns(tmpStreamed, 0, rb, rbIdx, numRepeatInFirstBatch, numStreamed)
      var j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRun(tmpStreamed, 0, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRun(tmpStreamed, incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRuns(tmpStreamed, 0, another, j, numRepeatInSecondBatch, numStreamed)


      batchJoinCopier.copyRightRepeats(tmpBuffered, 0, rb, rbIdx, numStreamed, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRepeat(tmpBuffered, i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRepeat(tmpBuffered, i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRepeats(tmpBuffered, i, another, j, numStreamed, numBuffered - i)
    }

    private def rightRunJoinTmp(): Unit = {
      genRightRunStatistics()

      batchJoinCopier.copyRightRuns(tmpBuffered, 0, rb, rbIdx, numRepeatInFirstBatch, numBuffered)
      var j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRun(tmpBuffered, 0, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRun(tmpBuffered, incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRuns(tmpBuffered, 0, another, j, numRepeatInSecondBatch, numBuffered)


      batchJoinCopier.copyLeftRepeats(tmpStreamed, 0, rb, rbIdx, numBuffered, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRepeat(tmpStreamed, i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRepeat(tmpStreamed, i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRepeats(tmpStreamed, i, another, j, numBuffered, numStreamed - i)
    }

    private def leftRunJoiningBufferedAllInTmp(): Unit = {
      genLeftRunStatistics()
      val numStreamedInCurrent = streamedRuns(0)

      batchJoinCopier.copyLeftRunsWithStep(tmpStreamed, 0, rb, rbIdx, numRepeatInFirstBatch, numStreamedInTmp, numStreamedInCurrent)
      batchJoinCopier.copyLeftRunsWithStep(streamedBatch, 0, rb, rbIdx + numStreamedInTmp, numRepeatInFirstBatch, numStreamedInCurrent, numStreamedInTmp)

      var j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        if (incompleteRunInFirstBatch > numStreamedInTmp) {
          batchJoinCopier.copyLeftRun(tmpStreamed, 0, rb, j, numStreamedInTmp)
          j += numStreamedInTmp
          batchJoinCopier.copyLeftRun(streamedBatch, 0, rb, j, incompleteRunInFirstBatch - numStreamedInTmp)
          batchJoinCopier.copyLeftRun(streamedBatch, incompleteRunInFirstBatch - numStreamedInTmp, another, 0, incompleteRunInSecondBatch)
        } else if (incompleteRunInFirstBatch == numStreamedInTmp) {
          batchJoinCopier.copyLeftRun(tmpStreamed, 0, rb, j, numStreamedInTmp)
          batchJoinCopier.copyLeftRun(streamedBatch, 0, another, 0, incompleteRunInSecondBatch)
        } else { // incompleteRunInFirstBatch < numStreamedInTmp
          batchJoinCopier.copyLeftRun(tmpStreamed, 0, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyLeftRun(tmpStreamed, incompleteRunInFirstBatch, another, 0, numStreamedInTmp - incompleteRunInFirstBatch)
          j = numStreamedInTmp - incompleteRunInFirstBatch
          batchJoinCopier.copyLeftRun(streamedBatch, 0, another, j, numStreamedInCurrent)
        }
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRunsWithStep(tmpStreamed, 0, another, j, numRepeatInSecondBatch, numStreamedInTmp, numStreamedInCurrent)
      batchJoinCopier.copyLeftRunsWithStep(streamedBatch, 0, another, j + numStreamedInTmp, numRepeatInSecondBatch, numStreamedInCurrent, numStreamedInTmp)

      batchJoinCopier.copyRightRepeats(tmpBuffered, 0, rb, rbIdx, numStreamed, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRepeat(tmpBuffered, i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRepeat(tmpBuffered, i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRepeats(tmpBuffered, i, another, j, numStreamed, numBuffered - i)
    }

    private def rightRunJoiningStreamedAllInTmp(): Unit = {
      genRightRunStatistics()
      val numBufferedInCurrent = bufferedRuns(0)

      batchJoinCopier.copyRightRunsWithStep(tmpBuffered, 0, rb, rbIdx, numRepeatInFirstBatch, numBufferedInTmp, numBufferedInCurrent)
      batchJoinCopier.copyRightRunsWithStep(bufferedBatch, 0, rb, rbIdx + numBufferedInTmp, numRepeatInFirstBatch, numBufferedInCurrent, numBufferedInTmp)

      var j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        if (incompleteRunInFirstBatch > numBufferedInTmp) {
          batchJoinCopier.copyRightRun(tmpBuffered, 0, rb, j, numBufferedInTmp)
          j += numBufferedInTmp
          batchJoinCopier.copyRightRun(bufferedBatch, 0, rb, j, incompleteRunInFirstBatch - numBufferedInTmp)
          batchJoinCopier.copyRightRun(bufferedBatch, incompleteRunInFirstBatch - numBufferedInTmp, another, 0, incompleteRunInSecondBatch)
        } else if (incompleteRunInFirstBatch == numBufferedInTmp) {
          batchJoinCopier.copyRightRun(tmpBuffered, 0, rb, j, numBufferedInTmp)
          batchJoinCopier.copyRightRun(bufferedBatch, 0, another, 0, incompleteRunInSecondBatch)
        } else { // incompleteRunInFirstBatch < numBufferedInTmp
          batchJoinCopier.copyRightRun(tmpBuffered, 0, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyRightRun(tmpBuffered, incompleteRunInFirstBatch, another, 0, numBufferedInTmp - incompleteRunInFirstBatch)
          j = numBufferedInTmp - incompleteRunInFirstBatch
          batchJoinCopier.copyRightRun(bufferedBatch, 0, another, j, numBufferedInCurrent)
        }
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRunsWithStep(tmpBuffered, 0, another, j, numRepeatInSecondBatch, numBufferedInTmp, numBufferedInCurrent)
      batchJoinCopier.copyRightRunsWithStep(bufferedBatch, 0, another, j + numBufferedInTmp, numRepeatInSecondBatch, numBufferedInCurrent, numBufferedInTmp)

      batchJoinCopier.copyLeftRepeats(tmpStreamed, 0, rb, rbIdx, numBuffered, numRepeatInFirstBatch)
      var i = numRepeatInFirstBatch; j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRepeat(tmpStreamed, i, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRepeat(tmpStreamed, i, another, 0, incompleteRunInSecondBatch)
        i += 1
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRepeats(tmpStreamed, i, another, j, numBuffered, numStreamed - i)
    }

    private def leftRunJoiningStreamedAllInTmp(): Unit = {
      genLeftRunStatistics()
      val numBufferedInCurrent = bufferedRuns(0)

      batchJoinCopier.copyLeftRuns(tmpStreamed, 0, rb, rbIdx, numRepeatInFirstBatch, numStreamed)
      var j = rbIdx + numRepeatInFirstBatch * numStreamed
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyLeftRun(tmpStreamed, 0, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyLeftRun(tmpStreamed, incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyLeftRuns(tmpStreamed, 0, another, j, numRepeatInSecondBatch, numStreamed)

      var i = 0
      if (numRepeatInFirstBatch > numBufferedInTmp) {
        batchJoinCopier.copyRightRepeats(tmpBuffered, 0, rb, rbIdx, numStreamed, numBufferedInTmp)
        j = rbIdx + numStreamed * numBufferedInTmp
        i = numRepeatInFirstBatch - numBufferedInTmp
        batchJoinCopier.copyRightRepeats(bufferedBatch, 0, rb, j, numStreamed, i)
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyRightRepeat(bufferedBatch, i, rb, rbIdx + numRepeatInFirstBatch * numStreamed, incompleteRunInFirstBatch)
          batchJoinCopier.copyRightRepeat(bufferedBatch, i, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyRightRepeats(bufferedBatch, i, another, j, numStreamed, numBufferedInCurrent - i)

      } else if (numRepeatInFirstBatch == numBufferedInTmp) {
        batchJoinCopier.copyRightRepeats(tmpBuffered, 0, rb, rbIdx, numStreamed, numBufferedInTmp)
        j = rbIdx + numStreamed * numBufferedInTmp
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyRightRepeat(bufferedBatch, 0, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyRightRepeat(bufferedBatch, 0, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyRightRepeats(bufferedBatch, i, another, j, numStreamed, numBufferedInCurrent - i)

      } else { // numRepeatInFirstBatch < numBufferedInTmp
        batchJoinCopier.copyRightRepeats(tmpBuffered, 0, rb, rbIdx, numStreamed, numRepeatInFirstBatch)
        j = rbIdx + numStreamed * numRepeatInFirstBatch
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyRightRepeat(tmpBuffered, numRepeatInFirstBatch, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyRightRepeat(tmpBuffered, numRepeatInFirstBatch, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyRightRepeats(tmpBuffered, numRepeatInFirstBatch + i, another, j, numStreamed, numBufferedInTmp - numRepeatInFirstBatch - i)
        j += (numBufferedInTmp - numRepeatInFirstBatch - i) * numStreamed
        batchJoinCopier.copyRightRepeats(bufferedBatch, 0, another, j, numStreamed, numBufferedInCurrent)
      }
    }

    private def rightRunJoiningBufferedAllInTmp(): Unit = {
      genRightRunStatistics()
      val numStreamedInCurrent = streamedRuns(0)

      batchJoinCopier.copyRightRuns(tmpBuffered, 0, rb, rbIdx, numRepeatInFirstBatch, numBuffered)
      var j = rbIdx + numRepeatInFirstBatch * numBuffered
      if (incompleteRunInFirstBatch > 0) {
        batchJoinCopier.copyRightRun(tmpBuffered, 0, rb, j, incompleteRunInFirstBatch)
        batchJoinCopier.copyRightRun(tmpBuffered, incompleteRunInFirstBatch, another, 0, incompleteRunInSecondBatch)
      }
      j = incompleteRunInSecondBatch
      batchJoinCopier.copyRightRuns(tmpBuffered, 0, another, j, numRepeatInSecondBatch, numBuffered)

      var i = 0
      if (numRepeatInFirstBatch > numStreamedInTmp) {
        batchJoinCopier.copyLeftRepeats(tmpStreamed, 0, rb, rbIdx, numBuffered, numStreamedInTmp)
        j = rbIdx + numBuffered * numStreamedInTmp
        i = numRepeatInFirstBatch - numStreamedInTmp
        batchJoinCopier.copyLeftRepeats(streamedBatch, 0, rb, j, numBuffered, i)
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyLeftRepeat(streamedBatch, i, rb, rbIdx + numRepeatInFirstBatch * numBuffered, incompleteRunInFirstBatch)
          batchJoinCopier.copyLeftRepeat(streamedBatch, i, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyLeftRepeats(streamedBatch, i, another, j, numBuffered, numStreamedInCurrent - i)

      } else if (numRepeatInFirstBatch == numStreamedInTmp) {
        batchJoinCopier.copyLeftRepeats(tmpStreamed, 0, rb, rbIdx, numBuffered, numStreamedInTmp)
        j = rbIdx + numBuffered * numStreamedInTmp
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyLeftRepeat(streamedBatch, 0, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyLeftRepeat(streamedBatch, 0, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyLeftRepeats(streamedBatch, i, another, j, numBuffered, numStreamedInCurrent - i)

      } else { // numRepeatInFirstBatch < numStreamedInTmp
        batchJoinCopier.copyLeftRepeats(tmpStreamed, 0, rb, rbIdx, numBuffered, numRepeatInFirstBatch)
        j = rbIdx + numBuffered * numRepeatInFirstBatch
        if (incompleteRunInFirstBatch > 0) {
          batchJoinCopier.copyLeftRepeat(tmpStreamed, numRepeatInFirstBatch, rb, j, incompleteRunInFirstBatch)
          batchJoinCopier.copyLeftRepeat(tmpStreamed, numRepeatInFirstBatch, another, 0, incompleteRunInSecondBatch)
          i += 1
        }
        j = incompleteRunInSecondBatch
        batchJoinCopier.copyLeftRepeats(tmpStreamed, numRepeatInFirstBatch + i, another, j, numBuffered, numStreamedInTmp - numRepeatInFirstBatch - i)
        j += (numStreamedInTmp - numRepeatInFirstBatch - i) * numBuffered
        batchJoinCopier.copyLeftRepeats(streamedBatch, 0, another, j, numBuffered, numStreamedInCurrent)
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
      if (streamedRunIsLonger) {
        while (rbIdx < rb.capacity && findMatches) {
          // copy rows into output
          numStreamed = streamedRuns(streamedRowIdx)
          numBuffered = bufferedRuns(bufferedRowIdx)
          numOutput = numStreamed * numBuffered
          genOutputLocation()

          // runs inside streamed and buffered
          if (numStreamed + streamedRowIdx < streamedSize &&
            numBuffered + bufferedRowIdx < bufferedSize) {

            leftRunJoin()
            advancedStreamed()
            advancedBuffered()

          } else if (numStreamed + streamedRowIdx < streamedSize) { // buffered span

            leftRunJoin()
            advancedBuffered()

          } else if (numBuffered + bufferedRowIdx < bufferedSize) { // streamed span

            leftRunJoin()
            advancedStreamed()

          } else { // both span
            // copy last key to tmp
            tmpStreamed.reset()
            streamedCopierToTmp.copy(streamedBatch, streamedRowIdx, tmpStreamed, 0, numStreamed)
            tmpBuffered.reset()
            bufferedCopierToTmp.copy(bufferedBatch, bufferedRowIdx, tmpBuffered, 0, numBuffered)

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
            genOutputLocation()

            if (!streamedKeyContinues && !bufferedKeyContinues) {
              leftRunJoinTmp()
            } else if (streamedKeyContinues && !bufferedKeyContinues) {
              leftRunJoiningBufferedAllInTmp()
            } else if (!streamedKeyContinues && bufferedKeyContinues) {
              leftRunJoiningStreamedAllInTmp()
            } else {
              joiningBothSpan()
            }
          }
          rbIdx += numRowsInFirstBatch
          anotherIdx += numRowsInSecondBatch

          findMatches = findNextInnerJoinRows()
        }
      } else { // buffered run is longer
        while (rbIdx < rb.capacity && findMatches) {
          // copy rows into output
          numStreamed = streamedRuns(streamedRowIdx)
          numBuffered = bufferedRuns(bufferedRowIdx)
          numOutput = numStreamed * numBuffered
          genOutputLocation()

          // runs inside streamed and buffered
          if (numStreamed + streamedRowIdx < streamedSize &&
            numBuffered + bufferedRowIdx < bufferedSize) {

            rightRunJoin()
            advancedStreamed()
            advancedBuffered()

          } else if (numStreamed + streamedRowIdx < streamedSize) { // buffered span

            rightRunJoin()
            advancedBuffered()

          } else if (numBuffered + bufferedRowIdx < bufferedSize) { // streamed span

            rightRunJoin()
            advancedStreamed()

          } else { // both span
            // copy last key to tmp
            tmpStreamed.reset()
            streamedCopierToTmp.copy(streamedBatch, streamedRowIdx, tmpStreamed, 0, numStreamed)
            tmpBuffered.reset()
            bufferedCopierToTmp.copy(bufferedBatch, bufferedRowIdx, tmpBuffered, 0, numBuffered)

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
            genOutputLocation()

            if (!streamedKeyContinues && !bufferedKeyContinues) {
              rightRunJoinTmp()
            } else if (streamedKeyContinues && !bufferedKeyContinues) {
              rightRunJoiningBufferedAllInTmp()
            } else if (!streamedKeyContinues && bufferedKeyContinues) {
              rightRunJoiningStreamedAllInTmp()
            } else {
              joiningBothSpan()
            }
          }
          rbIdx += numRowsInFirstBatch
          anotherIdx += numRowsInSecondBatch

          findMatches = findNextInnerJoinRows()
        }
      }
      numOutputRows += rbIdx
      rb.size = rbIdx
      if (rbIdx == 0) false else true
    }

    override def getBatch: RowBatch = rb
  }

  // scalastyle:on

  // read in the first batches to start comparison
  advancedStreamed()
  advancedBuffered()

  def identifyLongerRun(): Boolean = {
    val sn = streamedRuns.filter(_ != 0)
    val streamedRunAvg = if (sn.length == 0) 0 else sn.sum / sn.length
    val bn = bufferedRuns.filter(_ != 0)
    val bufferedRunAvg = if (bn.length == 0) 0 else bn.sum / bn.length
    if (streamedRunAvg >= bufferedRunAvg) true else false
  }

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

  def findNextOuterJoinRows(): Boolean = {
    if (streamedBatch == null) {
      false
    } else {
      var comp = 0
      do {
        comp = interBatchComparator.compare(
          streamedBatch, streamedRowIdx, bufferedBatch, bufferedRowIdx)
      } while (comp > 0 && advancedBuffered())
      true
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
