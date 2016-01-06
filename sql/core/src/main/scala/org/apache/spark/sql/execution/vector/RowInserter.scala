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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableRow, Attribute}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.vector.{ColumnVector, ColumnVectorUpdater}
import org.apache.spark.sql.types._

abstract class RowInserter {
  def insert(row: InternalRow, updaters: Seq[ColumnVectorUpdater]): Unit
}

abstract class RowGetter {
  def get(row: MutableRow, vectors: Seq[ColumnVector], rowIdx: Int): Unit
}

object GenerateRowGetter extends CodeGenerator[Seq[DataType], RowGetter] {
  override protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  override protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  val dataType = classOf[DataType].getName

  override protected def create(in: Seq[DataType]): RowGetter = {
    val ctx = newCodeGenContext()

    def nullSafeGetter(dt: DataType, idx: Int): String = {
      dt match {
        case IntegerType => s"row.setInt($idx, vectors[$idx].vector[rowIdx])"
        case LongType => s"row.setLong($idx, vectors[$idx].vector[rowIdx])"
        case DoubleType => s"row.setDouble($idx, vectors[$idx].vector[rowIdx])"
        case StringType => s"row.update($idx, vectors[$idx].vector[rowIdx])"
        case _ => s"NOT SUPPORTED TYPE"
      }
    }

    val getter = in.zipWithIndex.map { case (dt, idx) =>
      val nullSafe = nullSafeGetter(dt, idx)
      s"""
        if (vectors[$idx].isNull[rowIdx]) {
          row.setNullAt($idx)
        } else {
          $nullSafe
        }
       """
      }.mkString("\n")

    val code =
      s"""
        public java.lang.Object generate($dataType[] types) {
          return new SpecificRowGetter();
        }

        class SpecificRowGetter extends ${classOf[RowGetter].getName} {
          @Override
          public void get(MutableRow row, ColumnVector[] vectors, int rowIdx) {
            $getter
          }
        }
       """

    logDebug(s"code for ${in.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = rowBatchConverterCompile(code)
    c.generate(in.toArray).asInstanceOf[RowGetter]
  }
}

object GenerateRowInserter extends CodeGenerator[Seq[DataType], RowInserter] {
  override protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  override protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  val dataType = classOf[DataType].getName
  val cvUpdaterType = classOf[ColumnVectorUpdater].getName

  override protected def create(in: Seq[DataType]): RowInserter = {
    val ctx = newCodeGenContext()

    def nullSafeGetAndPut(dt: DataType, idx: Int): String = {
      dt match {
        case IntegerType => s"updaters[$idx].putInt(row.getInt($idx))"
        case LongType => s"updaters[$idx].putLong(row.getLong($idx))"
        case DoubleType => s"updaters[$idx].putDouble(row.getDouble($idx))"
        case StringType => s"updaters[$idx].putString(row.getUTF8String($idx))"
        case _ => s"NOT SUPPORTED TYPE"
      }
    }

    val getAndPut = in.zipWithIndex.map { case (dt, idx) =>
      val nullSafe = nullSafeGetAndPut(dt, idx)
      s"""
        if (row.isNullAt($idx)) {
          updaters[$idx].putNull()
        } else {
          $nullSafe
        }
       """
    }.mkString("\n")

    val code =
      s"""
        public java.lang.Object generate($dataType[] types) {
          return new SpecificRowInserter();
        }

        class SpecificRowInserter extends ${classOf[RowInserter].getName} {
          @Override
          public void insert(InternalRow row, $cvUpdaterType[] updaters) {
            $getAndPut
          }
        }
      """

    logDebug(s"code for ${in.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = rowBatchConverterCompile(code)
    c.generate(in.toArray).asInstanceOf[RowInserter]
  }
}
