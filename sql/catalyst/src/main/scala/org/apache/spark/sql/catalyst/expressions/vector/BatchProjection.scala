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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{ExpressionCanonicalizer, CodeGenerator}
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class BatchProjection {
  def apply(rowBatch: RowBatch): RowBatch
}

object BatchProjection {

  /**
   * Same as other create()'s but allowing enabling/disabling subexpression elimination.
   * TODO: refactor the plumbing and clean this up.
   */
  def create(
      exprs: Seq[Expression],
      inputSchema: Seq[Attribute],
      subexpressionEliminationEnabled: Boolean): BatchProjection = {
    val e = exprs.map(BindReferences.bindReference(_, inputSchema))
      .map(_ transform {
        case CreateStruct(children) => CreateStructUnsafe(children)
        case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
      })
    GenerateBatchProjection.generate(e, subexpressionEliminationEnabled)
  }
}

object GenerateBatchProjection extends CodeGenerator[Seq[Expression], BatchProjection] {
  /**
   * Canonicalizes an input expression. Used to avoid double caching expressions that differ only
   * cosmetically.
   */
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  /** Binds an input expression to a given input schema */
  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def generate(
    expressions: Seq[Expression],
    subexpressionEliminationEnabled: Boolean): BatchProjection = {
    create(expressions, subexpressionEliminationEnabled)
  }

  /**
    * Generates a class for a given input expression.  Called when there is not cached code
    * already available.
    */
  override protected def create(in: Seq[Expression]): BatchProjection =
    create(in, subexpressionEliminationEnabled = false)

  private def create(
      expressions: Seq[Expression],
      subexpressionEliminationEnabled: Boolean): BatchProjection = {
    val ctx = newCodeGenContext()
    val batchExpressions = expressions.map(exprToBatch)
    val exprEvals = ctx.generateBatchExpressions(batchExpressions, subexpressionEliminationEnabled)

    val result = ctx.freshName("result")
    ctx.addMutableState("RowBatch", result, s"this.$result = new RowBatch(${expressions.size});")

    // Reset the subexpression values for each row.
    val subexprReset = ctx.subExprResetVariables.mkString("\n")

    val evals = exprEvals.zipWithIndex.map { case (eval, index) =>
      s"""
        ${eval.code}
        $result.columns[$index] = ${eval.value};
      """
    }.mkString("\n")


    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchProjection(exprs);
      }

      class SpecificBatchProjection extends ${classOf[BatchProjection].getName} {

        private $exprType[] expressions;

        ${declareMutableStates(ctx)}

        ${declareAddedFunctions(ctx)}

        public SpecificBatchProjection($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public java.lang.Object apply(java.lang.Object rowBatch) {
          return apply((RowBatch) rowBatch);
        }

        public RowBatch apply(RowBatch ${ctx.INPUT_ROWBATCH}) {
          $subexprReset
          $evals
          return $result;
        }
      }
      """

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchProjection]
  }
}
