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
import org.apache.spark.sql.catalyst.expressions.{Expression, MutableRow, Attribute}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.vector.ColumnVector
import org.apache.spark.sql.types._

abstract class RowInserter {
  def insert(row: InternalRow, vectors: Array[ColumnVector], rowId: Int): Unit
}

abstract class RowGetter {
  def get(row: MutableRow, vectors: Array[ColumnVector], rowIdx: Int): Unit
}

object GenerateRowGetter extends CodeGenerator[Seq[Expression], RowGetter] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in

  override protected def create(in: Seq[Expression]): RowGetter = {
    val ctx = newCodeGenContext()

    def nullSafeGetter(dt: DataType, idx: Int): String = {
      dt match {
        case IntegerType => s"row.setInt($idx, vectors[$idx].intVector[rowIdx])"
        case LongType => s"row.setLong($idx, vectors[$idx].longVector[rowIdx])"
        case DoubleType => s"row.setDouble($idx, vectors[$idx].doubleVector[rowIdx])"
        case StringType => s"row.update($idx, vectors[$idx].stringVector[rowIdx])"
        case _ => s"NOT SUPPORTED TYPE"
      }
    }

    val getter = in.zipWithIndex.map { case (expr, idx) =>
      val nullSafe = nullSafeGetter(expr.dataType, idx)
      s"""
        if (vectors[$idx].isNull[rowIdx]) {
          row.setNullAt($idx);
        } else {
          $nullSafe;
        }
       """
      }.mkString("\n")

    val code =
      s"""
        public java.lang.Object generate($exprType[] exprs) {
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

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[RowGetter]
  }
}

object GenerateRowInserter extends CodeGenerator[Seq[Expression], RowInserter] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in
  override protected def create(in: Seq[Expression]): RowInserter = {
    val ctx = newCodeGenContext()

    def nullSafeGetAndPut(dt: DataType, idx: Int): String = {
      dt match {
        case IntegerType => s"vectors[$idx].putInt(rowId, row.getInt($idx))"
        case LongType => s"vectors[$idx].putLong(rowId, row.getLong($idx))"
        case DoubleType => s"vectors[$idx].putDouble(rowId, row.getDouble($idx))"
        case StringType => s"vectors[$idx].putString(rowId, row.getUTF8String($idx))"
        case _ => s"NOT SUPPORTED TYPE"
      }
    }

    val getAndPut = in.zipWithIndex.map { case (expr, idx) =>
      val nullSafe = nullSafeGetAndPut(expr.dataType, idx)
      s"""
        if (row.isNullAt($idx)) {
          vectors[$idx].putNull(rowId);
        } else {
          $nullSafe;
        }
       """
    }.mkString("\n")

    val code =
      s"""
        public java.lang.Object generate($exprType[] exprs) {
          return new SpecificRowInserter();
        }

        class SpecificRowInserter extends ${classOf[RowInserter].getName} {
          @Override
          public void insert(InternalRow row, ColumnVector[] vectors, int rowId) {
            $getAndPut
          }
        }
      """

    logDebug(s"code for ${in.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[RowInserter]
  }
}
