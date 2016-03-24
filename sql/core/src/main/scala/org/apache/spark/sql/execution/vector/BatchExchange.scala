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

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.vector.{BatchProjection, GenerateBatchWrite}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{ShuffledRowRDD, _}
import org.apache.spark.util.MutablePair

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
      new RowBatchSerializer(output)
    } else {
      new SparkSqlSerializer(sparkConf)
    }
  }
  override protected def doPrepare(): Unit = {
    // If an ExchangeCoordinator is needed, we register this Exchange operator
    // to the coordinator when we do prepare. It is important to make sure
    // we register this operator right before the execution instead of register it
    // in the constructor because it is possible that we create new instances of
    // Exchange operators when we transform the physical plan
    // (then the ExchangeCoordinator will hold references of unneeded Exchanges).
    // So, we should only call registerExchange just before we start to execute
    // the plan.
    coordinator match {
      case Some(exchangeCoordinator) => // exchangeCoordinator.registerExchange(this)
      case None =>
    }
  }

  /**
    * Returns a [[ShuffleDependency]] that will partition rows of its child based on
    * the partitioning scheme defined in `newPartitioning`. Those partitions of
    * the returned ShuffleDependency will be the input of shuffle.
    */
  private[sql] def prepareShuffleDependency():
      ShuffleDependency[Int, RowBatch, RowBatch] = {
    val rdd = child.batchExecute()
    val part: Partitioner = newPartitioning match {
      case HashPartitioning(_, n) =>
        new PartitionIdPassthrough(n)
      case _ => sys.error(s"BatchExchange not implemented for $newPartitioning")
    }
    def getPartitionKeyExtractor(): BatchProjection = newPartitioning match {
      case h: HashPartitioning =>
        BatchProjection.create(h.partitionIdExpression :: Nil, child.output)
      case _ => sys.error(s"BatchExchange not implemented for $newPartitioning")
    }

    def sortRowsByPartition(iterator: Iterator[RowBatch],
        numPartitions: Int,
        partitionKeyExtractor: BatchProjection): Iterator[Product2[Int, RowBatch]] = {
      val batchWrite = GenerateBatchWrite.generate(output)

      new Iterator[Product2[Int, RowBatch]] {
        var currentBatch: RowBatch = null
        var currentPair = new MutablePair[Int, RowBatch]()

        var currentPID: Int = -1
        var numRows: Int = 0
        var startIdx: Int = -1
        var currentIdx: Int = 0
        var currentSize: Int = -1
        var currentPartitionKeys: Array[Int] = null

        def findNextRange(): Unit = {
          if (currentBatch == null || currentIdx == currentSize) {
            currentBatch = iterator.next()
            currentPartitionKeys = partitionKeyExtractor(currentBatch).columns(0).intVector
            currentBatch.sort(currentPartitionKeys)

            currentPID = -1
            numRows = 0
            startIdx = -1
            currentIdx = 0
            currentSize = currentBatch.size
          }

          var i = currentBatch.sorted(currentIdx)
          currentPID = currentPartitionKeys(i)
          startIdx = currentIdx
          while(currentPartitionKeys(i) == currentPID && currentIdx < currentSize - 1) {
            numRows += 1
            currentIdx += 1
            i = currentBatch.sorted(currentIdx)
          }
          if (currentPartitionKeys(i) == currentPID && currentIdx == currentSize - 1) {
            numRows += 1
            currentIdx += 1
          }

          currentBatch.startIdx = startIdx
          currentBatch.numRows = numRows
          currentBatch.writer = batchWrite
          currentPair.update(currentPID, currentBatch)
          currentPID = -1
          numRows = 0
        }

        override def hasNext: Boolean = iterator.hasNext || currentIdx < currentSize

        override def next(): Product2[Int, RowBatch] = {
          findNextRange()
          currentPair
        }
      }
    }

    val rddWithPartitionCV: RDD[Product2[Int, RowBatch]] = {
      rdd.mapPartitions { iter =>
        val getPartitionKey = getPartitionKeyExtractor()
        sortRowsByPartition(iter, part.numPartitions, getPartitionKey)
      }
    }

    val dependency =
      new ShuffleDependency[Int, RowBatch, RowBatch](
        rddWithPartitionCV,
        part,
        Some(serializer))

    dependency
  }

  /**
    * Returns a [[ShuffledRowRDD]] that represents the post-shuffle dataset.
    * This [[ShuffledRowRDD]] is created based on a given [[ShuffleDependency]] and an optional
    * partition start indices array. If this optional array is defined, the returned
    * [[ShuffledRowRDD]] will fetch pre-shuffle partitions based on indices of this array.
    */
  private[sql] def preparePostShuffleRDD(
    shuffleDependency: ShuffleDependency[Int, RowBatch, RowBatch],
    specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowBatchRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowBatchRDD(shuffleDependency, specifiedPartitionStartIndices)
  }

  protected override def doBatchExecute(): RDD[RowBatch] = attachTree(this , "batchExecute") {
    coordinator match {
      case Some(exchangeCoordinator) =>
        // val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        // assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        // shuffleRDD
        null
      case None =>
        val shuffleDependency = prepareShuffleDependency()
        preparePostShuffleRDD(shuffleDependency)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException
}

object BatchExchange {
  def apply(newPartitioning: Partitioning, child: SparkPlan): BatchExchange = {
    BatchExchange(newPartitioning, child, coordinator = None: Option[ExchangeCoordinator])
  }
}
