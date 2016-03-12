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

package org.apache.spark.sql.execution.vector.aggregate

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{InternalAccumulator, Logging, TaskContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.vector.BatchProjection
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

abstract class BatchAggregateIterator(
    groupingExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection))
  extends Iterator[UnsafeRow] with Logging {

  /**
    * The following combinations of AggregationMode are supported:
    * - Partial
    * - PartialMerge (for single distinct)
    * - Partial and PartialMerge (for single distinct)
    * - Final
    * - Complete (for SortBasedAggregate with functions that does not support Partial)
    * - Final and Complete (currently not used)
    *
    * TODO: AggregateMode should have only two modes: Update and Merge, AggregateExpression
    * could have a flag to tell it's final or not.
    */
  {
    val modes = aggregateExpressions.map(_.mode).distinct.toSet
    require(modes.size <= 2,
      s"$aggregateExpressions are not supported because they have more than 2 distinct modes.")
    require(modes.subsetOf(Set(Partial, PartialMerge)) || modes.subsetOf(Set(Final, Complete)),
      s"$aggregateExpressions can't have Partial/PartialMerge and Final/Complete in the same time.")
  }

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected def initializeAggregateFunctions(
    expressions: Seq[AggregateExpression],
    startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset
    val functions = new Array[AggregateFunction](expressions.length)
    var i = 0
    while (i < expressions.length) {
      val func = expressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = expressions(i).mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)

  // The projection used to initialize buffer values for all expression-based aggregates.
  protected[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")

  protected val keyWrapper: BatchProjection =
    BatchProjection.create(V2R(groupingExpressions) :: Nil, inputAttributes, false)
  protected val hasher: BatchProjection =
    BatchProjection.create(new Murmur3Hash(groupingExpressions, 0) :: Nil, inputAttributes, false)

  protected val updater: BatchBufferUpdate =
    GenerateBatchBufferUpdate.generate(
      aggregateExpressions, inputAttributes, groupingExpressions.isEmpty)

  protected val groupingProjection: UnsafeProjection =
    UnsafeProjection.create(groupingExpressions, inputAttributes)
  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All imperative AggregateFunctions.
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  // Initializing the function used to generate the output row.
  protected def generateResultProjection(): (UnsafeRow, MutableRow) => UnsafeRow = {
    val joinedRow = new JoinedRow
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    if (modes.contains(Final) || modes.contains(Complete)) {
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val aggregateResult = new SpecificMutableRow(aggregateAttributes.map(_.dataType))
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
      expressionAggEvalProjection.target(aggregateResult)

      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)

      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        // Generate results for all expression-based aggregate functions.
        expressionAggEvalProjection(currentBuffer)
        // Generate results for all imperative aggregate functions.
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer))
          i += 1
        }
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ bufferAttributes,
        groupingAttributes ++ bufferAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else {
      // Grouping-only: we only output values of grouping expressions.
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  protected val generateOutput: (UnsafeRow, MutableRow) => UnsafeRow =
    generateResultProjection()
}

class HashBasedBatchAggregateIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[RowBatch],
    testFallbackStartsAt: Option[Int],
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
    dataSize: LongSQLMetric,
    spillSize: LongSQLMetric)
  extends BatchAggregateIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get().taskMemoryManager(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // reused buffers
  private[this] val buffers = Array.fill[UnsafeRow](RowBatch.DEFAULT_SIZE)(new UnsafeRow())

  private def processInputs(fallbackStartsAt: Int): Unit = {
    if (groupingExpressions.isEmpty) {
      val groupingKey = groupingProjection.apply(null)
      buffers(0).pointTo(hashMap.getAggregationBufferFromUnsafeRow(groupingKey));
      while (inputIter.hasNext) {
        val currentBatch = inputIter.next()
        numInputRows += currentBatch.size
        updater.update(currentBatch, buffers)
      }
    } else {
      while (inputIter.hasNext) {
        val currentBatch = inputIter.next()
        numInputRows += currentBatch.size
        val groupingKeys = keyWrapper(currentBatch).columns(0).rowVector
        val hashCodes = hasher(currentBatch).columns(0).intVector

        // TODO no enough spaces for hashmap to extend
        if (currentBatch.selectedInUse) {
          var j = 0
          while (j < currentBatch.size) {
            val i = currentBatch.selected(j)
            buffers(i).pointTo(
              hashMap.getAggregationBufferFromUnsafeRow(groupingKeys(i), hashCodes(i)))
            j += 1
          }
        } else {
          var i = 0
          while (i < currentBatch.size) {
            buffers(i).pointTo(
              hashMap.getAggregationBufferFromUnsafeRow(groupingKeys(i), hashCodes(i)))
            i += 1
          }
        }

        updater.update(currentBatch, buffers)
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  private[this] var mapIteratorHasNext: Boolean = false

  private[this] val sortBased: Boolean = false

  /**
   * Start processing input rows
   */
  processInputs(Int.MaxValue)

  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) {
      hashMap.free()
    }
  }

  private def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]): (Array[UnsafeRow], RowBatch) => Unit = {
    null
  }

  override final def hasNext: Boolean = {
    (!sortBased && mapIteratorHasNext)
  }

  override final def next(): UnsafeRow = {
    val result = generateOutput(
      aggregationBufferMapIterator.getKey,
      aggregationBufferMapIterator.getValue)

    // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
    // idempotent.
    mapIteratorHasNext = aggregationBufferMapIterator.next()

    val output = if (!mapIteratorHasNext) {
      // If there is no input from aggregationBufferMapIterator, we copy current result.
      val resultCopy = result.copy()
      // Then, we free the map.
      hashMap.free()
      resultCopy
    } else {
      result
    }

    // If this is the last record, update the task's peak memory usage. Since we destroy
    // the map to create the sorter, their memory usages should not overlap, so it is safe
    // to just use the max of the two.
    if (!hasNext) {
      val mapMemory = hashMap.getPeakMemoryUsedBytes
      dataSize += mapMemory
      spillSize += TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore
      TaskContext.get().internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(mapMemory)
    }
    numOutputRows += 1
    output
  }

  /**
    * Generate a output row when there is no input and there is no grouping expression.
    */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      val row = createNewAggregationBuffer()
      row.copyFrom(initialAggregationBuffer)
      // We create a output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), row).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
