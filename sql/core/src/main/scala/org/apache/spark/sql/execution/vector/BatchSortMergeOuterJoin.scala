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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, BoundReference, Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.vector._
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

case class BatchSortMergeOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  assert(condition.isEmpty, "Currently, we do not support outer join with bound condition yet.")

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

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  val leftKeyPositions = leftKeys.map(
    BindReferences.bindReference(_, left.output).asInstanceOf[BoundReference].ordinal)
  val rightKeyPositions = rightKeys.map(
    BindReferences.bindReference(_, right.output).asInstanceOf[BoundReference].ordinal)

  override def outputPartitioning: Partitioning = joinType match {
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => requiredOrders(leftKeys)
    case RightOuter => requiredOrders(rightKeys)
    // there are null rows in both streams, so there is no order
    case FullOuter => Nil
    case x => throw new IllegalArgumentException(
      s"SortMergeOuterJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

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
        defaultBatchCapacity)

      joinType match {
        case LeftOuter => smjScanner.getLeftOuterJoinedIterator().toScala
        case RightOuter => smjScanner.getRightOuterJoinedIterator().toScala
        case x => throw new IllegalArgumentException(
          s"SortMergeOuterJoin should not take $x as the JoinType")
      }
    }
  }

}
