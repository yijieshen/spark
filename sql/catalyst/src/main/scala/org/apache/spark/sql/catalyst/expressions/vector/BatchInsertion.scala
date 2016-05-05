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
import org.apache.spark.sql.types._

abstract class BatchInsertion {
  def insert(from: RowBatch, to: RowBatch, start: Int, length: Int): Unit
}

object GenerateBatchInsertion extends CodeGenerator[Seq[Expression], BatchInsertion] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(
    in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  def generate(in: Seq[Expression], defaultCapacity: Int): BatchInsertion = {
    create(canonicalize(in), defaultCapacity)
  }

  override protected def create(in: Seq[Expression]): BatchInsertion =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Seq[Expression], defaultCapacity: Int): BatchInsertion = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val schema = in.map(_.dataType)

    val columnInsertions = schema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntCV(from.columns[$idx], from.sorted, start, length);"
        case LongType =>
          s"to.columns[$idx].putLongCV(from.columns[$idx], from.sorted, start, length);"
        case DoubleType =>
          s"to.columns[$idx].putDoubleCV(from.columns[$idx], from.sorted, start, length);"
        case StringType =>
          s"to.columns[$idx].putStringCV(from.columns[$idx], from.sorted, start, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchInsertion(exprs);
      }

      class SpecificBatchInsertion extends ${classOf[BatchInsertion].getName} {
        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchInsertion($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public void insert(RowBatch from, RowBatch to, int start, int length) {
          to.size += length;
          $columnInsertions
        }
      }
    """

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchInsertion]
  }
}
