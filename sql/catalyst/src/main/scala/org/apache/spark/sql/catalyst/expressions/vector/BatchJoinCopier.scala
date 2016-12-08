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
package org.apache.spark.sql.catalyst.expressions.vector

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

abstract class BatchJoinCopier {
  def copyLeft(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int)
  def copyRight(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int)
}

object GenerateBatchJoinCopier extends CodeGenerator[Seq[Seq[Expression]], BatchJoinCopier] {

  override protected def create(
    in: Seq[Seq[Expression]]): BatchJoinCopier = throw new UnsupportedOperationException

  override protected def canonicalize(in: Seq[Seq[Expression]]): Seq[Seq[Expression]] = in

  override protected def bind(
    in: Seq[Seq[Expression]],
    inputSchema: Seq[Attribute]): Seq[Seq[Expression]] = in

  def generate(in: Seq[Seq[Expression]], defaultCapacity: Int): BatchJoinCopier = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val leftSchema = in(0).map(_.dataType)
    val rightSchema = in(1).map(_.dataType)
    val rightOffset = in(0).size

    val leftCopiers = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putInt(toIdx, from.columns[$idx].getInt(fromIdx));"
        case LongType =>
          s"to.columns[$idx].putLong(toIdx, from.columns[$idx].getLong(fromIdx));"
        case DoubleType =>
          s"to.columns[$idx].putDouble(toIdx, from.columns[$idx].getDouble(fromIdx));"
        case StringType =>
          s"to.columns[$idx].putString(toIdx, from.columns[$idx].getString(fromIdx));"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val rightCopiers = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx +$rightOffset].putInt(toIdx, from.columns[$idx].getInt(fromIdx));"
        case LongType =>
          s"to.columns[$idx +$rightOffset].putLong(toIdx, from.columns[$idx].getLong(fromIdx));"
        case DoubleType =>
          s"to.columns[$idx +$rightOffset].putDouble(toIdx, from.columns[$idx].getDouble(fromIdx));"
        case StringType =>
          s"to.columns[$idx +$rightOffset].putString(toIdx, from.columns[$idx].getString(fromIdx));"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchJoinCopier(exprs);
      }

      class SpecificBatchJoinCopier extends ${classOf[BatchJoinCopier].getName} {
        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchJoinCopier($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public void copyLeft(RowBatch from, int fromIdx, RowBatch to, int toIdx) {
          $leftCopiers
        }

        public void copyRight(RowBatch from, int fromIdx, RowBatch to, int toIdx) {
          $rightCopiers
        }
      }
    """

    val c = CodeGenerator.compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchJoinCopier]
  }
}
