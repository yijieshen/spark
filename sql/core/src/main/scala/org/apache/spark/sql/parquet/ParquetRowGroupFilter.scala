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

package org.apache.spark.sql.parquet

import java.util.{List => JList}

import org.apache.spark.Logging
import parquet.Preconditions._
import parquet.column.statistics.Statistics
import parquet.common.schema.ColumnPath
import parquet.filter2.predicate.Operators._
import parquet.filter2.predicate._
import parquet.schema.MessageType
import parquet.filter2.compat.FilterCompat._
import parquet.hadoop.metadata.{ColumnChunkMetaData, BlockMetaData}

import scala.collection.JavaConversions._
import scala.collection.mutable


class ParquetRowGroupFilter(
    private val blocks: JList[BlockMetaData],
    private val schema: MessageType) extends Visitor[JList[BlockMetaData]] with Logging{

  def visit(filterPredicateCompat: FilterPredicateCompat) = {
    val filterPredicate: FilterPredicate = filterPredicateCompat.getFilterPredicate

    // check that the schema of the filter matches the schema of the file
    SchemaCompatibilityValidator.validate(filterPredicate, schema)

    val filteredBlocks = mutable.ListBuffer.empty[BlockMetaData]
    for (block <- blocks) {
      if (!ParquetStatisticsFilter.canDrop(filterPredicate, block.getColumns)) {
        filteredBlocks += block
      }
    }

    filteredBlocks
  }

  def visit(unboundRecordFilterCompat: UnboundRecordFilterCompat) = blocks

  def visit(noOpFilter: NoOpFilter) = blocks
}

object ParquetRowGroupFilter {
  def filterRowGroups(filter: Filter, blocks: JList[BlockMetaData], schema: MessageType) = {
    checkNotNull(filter, "filter");
    filter.accept(new ParquetRowGroupFilter(blocks, schema));
  }
}

class ParquetStatisticsFilter(columnsList: JList[ColumnChunkMetaData])
  extends FilterPredicate.Visitor[Boolean] with Logging{

  val columns = mutable.Map[ColumnPath, ColumnChunkMetaData]()
  for (ccmd <- columnsList) {
    columns += (ccmd.getPath -> ccmd)
  }

  private def getColumnChunk(columnPath: ColumnPath): ColumnChunkMetaData = {
    val c = columns(columnPath)
    checkArgument(c != null, "Column " + columnPath.toDotString + " not found in schema!")
    c
  }

  // is this column chunk composed entirely of nulls?
  private def isAllNulls(column: ColumnChunkMetaData): Boolean = {
    return column.getStatistics.getNumNulls == column.getValueCount
  }

  // are there any nulls in this column chunk?
  private def hasNulls(column: ColumnChunkMetaData): Boolean = {
    return column.getStatistics.getNumNulls > 0
  }


  @Override def visit[T <: Comparable[T]](eq: Eq[T]): Boolean = {

    val filterColumn = eq.getColumn
    val value = eq.getValue
    val columnChunk: ColumnChunkMetaData = getColumnChunk(filterColumn.getColumnPath)

    if (value == null) {
      return !hasNulls(columnChunk)
    }

    if (isAllNulls(columnChunk)) {
      return true
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if value < min || value > max
    value.compareTo(stats.genericGetMin) < 0 || value.compareTo(stats.genericGetMax) > 0
  }

  @Override def visit[T <: Comparable[T]](notEq: NotEq[T]): Boolean = {
    val filterColumn= notEq.getColumn
    val value = notEq.getValue
    val columnChunk = getColumnChunk(filterColumn.getColumnPath)

    if (value == null) {
      return isAllNulls(columnChunk)
    }

    if (hasNulls(columnChunk)) {
      return false
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if this is a column where min = max = value
    return value.compareTo(stats.genericGetMin) == 0 && value.compareTo(stats.genericGetMax) == 0
  }

  @Override def visit[T <: Comparable[T]](lt: Lt[T]): Boolean = {
    val filterColumn = lt.getColumn
    val value = lt.getValue
    val columnChunk = getColumnChunk(filterColumn.getColumnPath)

    if (isAllNulls(columnChunk)) {
      return true
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if value <= min
    value.compareTo(stats.genericGetMin) <= 0
  }

  @Override def visit[T <: Comparable[T]](ltEq: LtEq[T]): Boolean = {
    val filterColumn = ltEq.getColumn
    val value = ltEq.getValue
    val columnChunk = getColumnChunk(filterColumn.getColumnPath)

    if (isAllNulls(columnChunk)) {
      return true
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if value < min
    return value.compareTo(stats.genericGetMin) < 0
  }

  @Override def visit[T <: Comparable[T]](gt: Gt[T]): Boolean = {
    val filterColumn = gt.getColumn
    val value = gt.getValue
    val columnChunk = getColumnChunk(filterColumn.getColumnPath)

    if (isAllNulls(columnChunk)) {
      return true
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if value >= max
    value.compareTo(stats.genericGetMax()) >= 0
  }

  @Override def visit[T <: Comparable[T]](gtEq: GtEq[T]): Boolean = {
    val filterColumn = gtEq.getColumn
    val value = gtEq.getValue
    val columnChunk = getColumnChunk(filterColumn.getColumnPath)

    if (isAllNulls(columnChunk)) {
      return true
    }

    val stats = columnChunk.getStatistics.asInstanceOf[Statistics[T]]

    // drop if value > max
    return value.compareTo(stats.genericGetMax()) > 0
  }

  @Override def visit(and: And): Boolean = {
    and.getLeft.accept(this) || and.getRight.accept(this)
  }

  @Override def visit(or: Or): Boolean = {
    or.getLeft.accept(this) && or.getRight.accept(this)
  }

  @Override def visit(not: Not): Boolean = {
    throw new IllegalArgumentException(
      "This predicate contains a not! " +
        "Did you forget to run this predicate through LogicalInverseRewriter? " +
        not)
  }

  // Not supported for now
  @Override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
    udp: UserDefined[T, U]): Boolean = {
    false
  }

  // Not supported for now
  @Override def visit[T <: Comparable[T], U <: UserDefinedPredicate[T]](
    udp: LogicalNotUserDefined[T, U]): Boolean = {
    false
  }
}

object ParquetStatisticsFilter {
  def canDrop(pred: FilterPredicate, columns: JList[ColumnChunkMetaData]): Boolean = {
    checkNotNull(pred, "pred")
    checkNotNull(columns, "columns")

    return pred.accept(new ParquetStatisticsFilter(columns))
  }
}
