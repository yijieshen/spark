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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.vector.{BatchProjection, GenerateBatchInsertion}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution._

case class BufferedBatchExchange(
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

    val simpleNodeName = if (tungstenMode) "BufferedBatchExchange" else "Exchange"
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
  @transient private lazy val bypassThreshold =
    sparkConf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)

  val rbSchema = output.map(_.dataType).toArray

  private val serializer: Serializer = {
    if (tungstenMode) {
      new DirectRowBatchSerializer(rbSchema)
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

    def sortRowsByPartition(
        iterator: Iterator[RowBatch],
        numPartitions: Int,
        partitionKeyExtractor: BatchProjection): Iterator[Product2[Int, RowBatch]] = {
      val batchInsertion = GenerateBatchInsertion.generate(output)

      new Iterator[Product2[Int, RowBatch]] {
        val rbBuffers = new Array[RowBatch](numPartitions)
        val fullBatches = new mutable.Queue[(Int, RowBatch)]

        var currentPair: Product2[Int, RowBatch] = null

        def populateRBs(): Unit = {
          while (iterator.hasNext && fullBatches.isEmpty) {
            val current = iterator.next()
            val partitionKey = partitionKeyExtractor(current).columns(0).intVector
            current.sort(partitionKey)

            var curPID: Int = -1    // currentPartitionId
            var numRows: Int = 0    // numRowsInCurrentPartition
            var startIdx: Int = -1  // currentPartitionStartIdx
            var j = 0
            while (j < current.size) {
              var i = current.sorted(j)
              curPID = partitionKey(i)
              startIdx = j

              while (partitionKey(i) == curPID && j < current.size - 1) {
                numRows += 1
                j += 1
                i = current.sorted(j)
              }
              if (partitionKey(i) == curPID && j == current.size - 1) {
                numRows += 1
                j += 1
              }

              if (rbBuffers(curPID) == null) {
                rbBuffers(curPID) = RowBatchPool.get()
              }
              if (rbBuffers(curPID).size + numRows > rbBuffers(curPID).capacity) {
                val recordsInserted = rbBuffers(curPID).capacity - rbBuffers(curPID).size
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, recordsInserted)
                fullBatches.enqueue((curPID, rbBuffers(curPID)))
                rbBuffers(curPID) = RowBatchPool.get()
                batchInsertion.insert(
                  current, rbBuffers(curPID), startIdx + recordsInserted, numRows - recordsInserted)
              } else if (rbBuffers(curPID).size + numRows == rbBuffers(curPID).capacity) {
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, numRows)
                fullBatches.enqueue((curPID, rbBuffers(curPID)))
                rbBuffers(curPID) = null
              } else {
                batchInsertion.insert(current, rbBuffers(curPID), startIdx, numRows)
              }

              curPID = -1
              numRows = 0
            }
          }
        }

        override def hasNext: Boolean =
          iterator.hasNext || !fullBatches.isEmpty || rbBuffers.exists(_ != null)

        override def next(): Product2[Int, RowBatch] = {
          if (currentPair != null) {
            RowBatchPool.put(currentPair._2)
            currentPair = null
          }

          if (!fullBatches.isEmpty) {
            currentPair = fullBatches.dequeue()
          } else if (iterator.hasNext) {
            populateRBs()
            if (fullBatches.isEmpty) {
              var i = 0
              while (i < rbBuffers.size) {
                if (rbBuffers(i) != null) {
                  fullBatches.enqueue((i, rbBuffers(i)))
                  rbBuffers(i) = null
                }
                i += 1
              }
            }
            currentPair = fullBatches.dequeue()
          } else if (rbBuffers.exists(_ != null)) {
            var i = 0
            while (i < rbBuffers.size) {
              if (rbBuffers(i) != null) {
                fullBatches.enqueue((i, rbBuffers(i)))
                rbBuffers(i) = null
              }
              i += 1
            }
            currentPair = fullBatches.dequeue()
          } else {
            sys.error("we should never reach here")
          }
          currentPair
        }
      }
    }
    val rddWithPartitionCV: RDD[Product2[Int, RowBatch]] = {
      if (part.numPartitions <= bypassThreshold) {
        rdd.mapPartitions { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          sortRowsByPartition(iter, part.numPartitions, getPartitionKey)
        }
      } else {
        sqlContext.sparkContext.emptyRDD
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

  object RowBatchPool {
    val pool = new mutable.Queue[RowBatch]
    def get(): RowBatch = {
      if (pool.isEmpty) {
        new RowBatch(rbSchema);
      } else {
        pool.dequeue()
      }
    }
    def put(rb: RowBatch): Unit = {
      rb.clear()
      pool.enqueue(rb)
    }
  }
}

object BufferedBatchExchange {
  def apply(newPartitioning: Partitioning, child: SparkPlan): BufferedBatchExchange = {
    BufferedBatchExchange(newPartitioning, child, coordinator = None: Option[ExchangeCoordinator])
  }
}
