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

import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.vector._
import org.apache.spark.sql.catalyst.expressions.vector.aggregate._
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Attribute, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{ExpressionCanonicalizer, CodeGenerator}
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class BatchBufferUpdate {
  def update(input: RowBatch, buffers: Array[UnsafeRow]): Unit
}

object GenerateBatchBufferUpdate extends CodeGenerator[Seq[Expression], BatchBufferUpdate] {
  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
      in: Seq[Expression],
      inputSchema: Seq[Attribute],
      noGroupingExpr: Boolean): BatchBufferUpdate =
    create(canonicalize(bind(in, inputSchema)), noGroupingExpr)

  protected def create(expressions: Seq[Expression]): BatchBufferUpdate =
    create(expressions, false)

  protected def create(expressions: Seq[Expression], noGroupingExpr: Boolean): BatchBufferUpdate = {
    val ctx = newCodeGenContext()

    val updateClass = classOf[BatchBufferUpdate].getName

    val aggFuncs = expressions.map(_.asInstanceOf[AggregateExpression].aggregateFunction)

    var i = 0
    val batchAggFuncs = aggFuncs.map { case func =>
      func match {
        case s: Sum =>
          val x = BatchSum(exprToBatch(s.child), s, i, noGroupingExpr)
          i += 1
          x
        case c: Count =>
          val x = BatchCount(c.children.map(exprToBatch), c, i, noGroupingExpr)
          i += 1
          x
        case a: Average =>
          val x = BatchAverage(exprToBatch(a.child), a, i, noGroupingExpr)
          i += 2
          x
        case ma: Max =>
          val x = BatchMax(exprToBatch(ma.child), ma, i, noGroupingExpr)
          i += 1
          x
        case mi: Min =>
          val x = BatchMin(exprToBatch(mi.child), mi, i, noGroupingExpr)
          i += 1
          x
        case _ => throw new NotImplementedException
      }
    }

    val evals = batchAggFuncs.map(_.gen(ctx).code).mkString("\n")

    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchBufferUpdate(exprs);
      }

      class SpecificBatchBufferUpdate extends $updateClass {
        private $exprType[] expressions;

        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchBufferUpdate($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public void update(RowBatch ${ctx.INPUT_ROWBATCH}, UnsafeRow[] ${ctx.BUFFERS}) {
          $evals
        }
      }
    """
    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchBufferUpdate]
  }
}
