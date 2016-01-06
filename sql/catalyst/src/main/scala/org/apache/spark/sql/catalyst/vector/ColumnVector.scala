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

import java.util.Arrays

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class ColumnVector {

  val isNull: Array[Boolean] = new Array[Boolean](size)

  var noNulls: Boolean = true

  var isRepeating: Boolean = false

  type fieldType <: Any

  def vector: Array[fieldType]

  def size: Int

  def reset(): Unit = {
    if (noNulls == false) {
      Arrays.fill(isNull, false)
    }
    noNulls = true
    isRepeating = false
  }

  def getUpdater(): ColumnVectorUpdater

  def null_value: fieldType
  def one_value: fieldType
}

case class IntColumnVector(size: Int) extends ColumnVector {
  type fieldType = IntegerType.InternalType
  val vector = new Array[fieldType](size)
  override def getUpdater(): ColumnVectorUpdater = new IntColumnVectorUpdater(this)
  override def null_value: Int = 1
  override def one_value: Int = 1
}

case class LongColumnVector(size: Int) extends ColumnVector {
  type fieldType = LongType.InternalType
  val vector = new Array[fieldType](size)
  override def getUpdater(): ColumnVectorUpdater = new LongColumnVectorUpdater(this)
  override def null_value: Long = 1L
  override def one_value: Long = 1L
}

case class DoubleColumnVector(size: Int) extends ColumnVector {
  type fieldType = DoubleType.InternalType
  val vector = new Array[fieldType](size)
  override def getUpdater(): ColumnVectorUpdater = new DoubleColumnVectorUpdater(this)
  override def null_value: Double = Double.NaN
  override def one_value: Double = 1.0
}

case class StringColumnVector(size: Int) extends ColumnVector {
  type fieldType = StringType.InternalType
  val vector = new Array[fieldType](size)
  override def getUpdater(): ColumnVectorUpdater = new StringColumnVectorUpdater(this)
  override def null_value: UTF8String = null
  override def one_value: UTF8String = null
}
