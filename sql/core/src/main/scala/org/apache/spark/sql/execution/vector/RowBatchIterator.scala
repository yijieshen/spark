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

import java.util.NoSuchElementException

import org.apache.spark.sql.catalyst.vector.RowBatch

/**
 * An internal iterator interface which presents a more restrictive API than
 * [[scala.collection.Iterator]].
 *
 * One major departure from the Scala iterator API is the fusing of the `hasNext()` and `next()`
 * calls: Scala's iterator allows users to call `hasNext()` without immediately advancing the
 * iterator to consume the next row, whereas RowIterator combines these calls into a single
 * [[advanceNext()]] method.
 */
private[sql] abstract class RowBatchIterator {
  /**
   * Advance this iterator by a single row. Returns `false` if this iterator has no more rows
   * and `true` otherwise. If this returns `true`, then the new row can be retrieved by calling
   * [[getBatch]].
   */
  def advanceNext(): Boolean

  /**
   * Retrieve the row from this iterator. This method is idempotent. It is illegal to call this
   * method after [[advanceNext()]] has returned `false`.
   */
  def getBatch: RowBatch

  /**
   * Convert this RowIterator into a [[scala.collection.Iterator]].
   */
  def toScala: Iterator[RowBatch] = new RowBatchIteratorToScala(this)
}

object RowBatchIterator {
  def fromScala(scalaIter: Iterator[RowBatch]): RowBatchIterator = {
    scalaIter match {
      case wrappedRowIter: RowBatchIteratorToScala => wrappedRowIter.rowIter
      case _ => new RowBatchIteratorFromScala(scalaIter)
    }
  }
}

private final class RowBatchIteratorToScala(val rowIter: RowBatchIterator)
    extends Iterator[RowBatch] {
  private [this] var hasNextWasCalled: Boolean = false
  private [this] var _hasNext: Boolean = false
  override def hasNext: Boolean = {
    // Idempotency:
    if (!hasNextWasCalled) {
      _hasNext = rowIter.advanceNext()
      hasNextWasCalled = true
    }
    _hasNext
  }
  override def next(): RowBatch = {
    if (!hasNext) throw new NoSuchElementException
    hasNextWasCalled = false
    rowIter.getBatch
  }
}

private final class RowBatchIteratorFromScala(scalaIter: Iterator[RowBatch])
    extends RowBatchIterator {

  private[this] var _next: RowBatch = null
  override def advanceNext(): Boolean = {
    if (scalaIter.hasNext) {
      _next = scalaIter.next()
      true
    } else {
      _next = null
      false
    }
  }
  override def getBatch: RowBatch = _next
  override def toScala: Iterator[RowBatch] = scalaIter
}
