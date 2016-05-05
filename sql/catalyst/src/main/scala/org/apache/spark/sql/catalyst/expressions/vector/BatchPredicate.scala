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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class BatchPredicate {
  def eval(rowBatch: RowBatch): Unit
}

object GenerateBatchPredicate extends CodeGenerator[Expression, BatchPredicate] {
  override protected def canonicalize(in: Expression): Expression =
    ExpressionCanonicalizer.execute(in)

  override protected def bind(in: Expression, inputSchema: Seq[Attribute]): Expression =
    BindReferences.bindReference(in, inputSchema)

  def generate(
      in: Expression,
      inputSchema: Seq[Attribute],
      defaultCapacity: Int): BatchPredicate = {
    create(canonicalize(bind(in, inputSchema)), defaultCapacity)
  }

  override protected def create(in: Expression): BatchPredicate =
    create(in, RowBatch.DEFAULT_CAPACITY)

  protected def create(in: Expression, defaultCapacity: Int): BatchPredicate = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val batchExpr = exprToBatch(in)
    val eval = batchExpr.gen(ctx)
    val code = s"""
      public SpecificBatchPredicate generate($exprType[] expr) {
        return new SpecificBatchPredicate(expr);
      }

      class SpecificBatchPredicate extends ${classOf[BatchPredicate].getName} {
        private final $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchPredicate($exprType[] expr) {
          this.expressions = expr;
          ${initMutableStates(ctx)}
        }

        public void eval(RowBatch ${ctx.INPUT_ROWBATCH}) {
          ${eval.code}
        }
      }
      """
    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchPredicate]
  }
}
