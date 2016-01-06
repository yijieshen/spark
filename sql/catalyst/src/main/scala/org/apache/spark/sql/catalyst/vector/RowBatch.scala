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

package org.apache.spark.sql.catalyst.vector

import org.apache.spark.sql.types._

/**
  *
  * @param numCols number of columns
  * @param capacity max number of rows it can hold
  */
class RowBatch(val numCols: Int, val capacity: Int) {
  def this(numCols: Int) = this(numCols, RowBatch.DEFAULT_SIZE)

  val selected: Array[Int] = new Array[Int](capacity) // only the first `size` elements are valid
  var selectedInUse: Boolean = false
  var size: Int = 0 // number of rows that qualify (hasn't been filtered out)

  val columns: Array[ColumnVector] = new Array[ColumnVector](numCols)

  var endOfFile: Boolean = false

  def reset(): Unit = {
    selectedInUse = false
    size = 0
    endOfFile = false
    for (col <- columns) {
      col.reset()
    }
  }

  def updaters(): Seq[ColumnVectorUpdater] = {
    columns.map(_.getUpdater())
  }
}

object RowBatch {
  val DEFAULT_SIZE = 1024

  def create(dataTypes: Seq[DataType], capacity: Int): RowBatch = {
    val rb = new RowBatch(dataTypes.size)
    dataTypes.zipWithIndex.foreach { case (dt, idx) =>
      rb.columns(idx) = dt match {
        case IntegerType => new IntColumnVector(capacity)
        case LongType => new LongColumnVector(capacity)
        case DoubleType => new DoubleColumnVector(capacity)
        case StringType => new StringColumnVector(capacity)
        case _ => throw new UnsupportedOperationException("no other type of CV supported yet")
      }
    }
    rb
  }

  def create(dataTypes: Seq[DataType]): RowBatch = create(dataTypes, DEFAULT_SIZE)
}
