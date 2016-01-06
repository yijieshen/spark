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

import org.apache.spark.unsafe.types.UTF8String

abstract class ColumnVectorUpdater {

  def cv: ColumnVector

  var idx: Int = 0

  def putNull(): Unit = {
    cv.isNull(idx) = true
    idx += 1
  }

  def reset(): Unit = {
    cv.reset()
    idx = 0
  }

  def putInt(value: Int): Unit =
    throw new UnsupportedOperationException(getClass.getName)
  def putLong(value: Long): Unit =
    throw new UnsupportedOperationException(getClass.getName)
  def putDouble(value: Double): Unit =
    throw new UnsupportedOperationException(getClass.getName)
  def putString(value: UTF8String): Unit =
    throw new UnsupportedOperationException(getClass.getName)
}

class IntColumnVectorUpdater(val cv: IntColumnVector) extends ColumnVectorUpdater {
  override def putInt(value: Int): Unit = {
    cv.vector(idx) = value
    idx += 1
  }
}

class LongColumnVectorUpdater(val cv: LongColumnVector) extends ColumnVectorUpdater {
  override def putLong(value: Long): Unit = {
    cv.vector(idx) = value
    idx += 1
  }
}

class DoubleColumnVectorUpdater(val cv: DoubleColumnVector) extends ColumnVectorUpdater {
  override def putDouble(value: Double): Unit = {
    cv.vector(idx) = value
    idx += 1
  }
}

class StringColumnVectorUpdater(val cv: StringColumnVector) extends ColumnVectorUpdater {
  override def putString(value: UTF8String): Unit = {
    cv.vector(idx) = value
    idx += 1
  }
}
