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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenContext, CodeGenerator, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class InterBatchComparator {
  def compare(lb: RowBatch, li: Int, rb: RowBatch, ri: Int): Int
}

object GenerateInterBatchComparator
    extends CodeGenerator[Seq[Seq[SortOrder]], InterBatchComparator] {

  override protected def canonicalize(
      in: Seq[Seq[SortOrder]]): Seq[Seq[SortOrder]] = {

    in(0).map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder]) ::
    in(1).map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder]) :: Nil
  }

  override protected def bind(
      in: Seq[Seq[SortOrder]],
      inputSchema: Seq[Attribute]): Seq[Seq[SortOrder]] =
    throw new UnsupportedOperationException

  protected def bind2(
      in: Seq[Seq[SortOrder]],
      inputSchema: Seq[Seq[Attribute]]): Seq[Seq[SortOrder]] = {

    in(0).map(BindReferences.bindReference(_, inputSchema(0))) ::
    in(1).map(BindReferences.bindReference(_, inputSchema(1))) :: Nil
  }

  def generate(
      orders: Seq[Seq[SortOrder]],
      schemas: Seq[Seq[Attribute]],
      defaultCapacity: Int): InterBatchComparator = {
    assert(orders.size == 2 && schemas.size == 2, "sort order and schema should both be two parts")
    create(canonicalize(bind2(orders, schemas)), defaultCapacity)
  }

  override protected def create(in: Seq[Seq[SortOrder]]): InterBatchComparator =
    create(in, RowBatch.DEFAULT_CAPACITY)

  def genComparisons(ctx: CodeGenContext, orders: Seq[Seq[SortOrder]]): String = {
    orders(0).zip(orders(1)).map { case (lso: SortOrder, rso: SortOrder) =>
      ctx.INPUT_ROWBATCH = "lb"
      val evalL = exprToBatch(lso.child).gen(ctx)
      ctx.INPUT_ROWBATCH = "rb"
      val evalR = exprToBatch(rso.child).gen(ctx)
      val asc = lso.direction == Ascending
      val dt = lso.child.dataType
      val pa = ctx.freshName("primitiveA")
      val pb = ctx.freshName("primitiveB")
      val get = ctx.getMethodName(dt)

      s"""
        ${evalL.code}
        ${evalR.code}
        ${ctx.javaType(dt)} $pa;
        ${ctx.javaType(dt)} $pb;

        $pa = ${evalL.value}.$get(li);
        $pb = ${evalR.value}.$get(ri);

        comp = ${ctx.genComp(dt, pa, pb)};
        if (comp != 0) {
          return ${if (asc) "comp" else "-comp"};
        }
      """
    }.mkString("\n")
  }

  protected def create(in: Seq[Seq[SortOrder]], defaultCapacity: Int): InterBatchComparator = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val comparisons = genComparisons(ctx, in)
    val code = s"""
      public SpecificInterBatchComparator generate($exprType[] expr) {
        return new SpecificInterBatchComparator(expr);
      }

      class SpecificInterBatchComparator extends ${classOf[InterBatchComparator].getName} {
        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificInterBatchComparator($exprType[] expr) {
          expressions = expr;
          ${initMutableStates(ctx)}
        }

        public int compare(RowBatch lb, int li, RowBatch rb, int ri) {
          int comp;
          $comparisons
          return 0;
        }
      }
    """
    logDebug(s"Generated Comparator: ${CodeFormatter.format(code)}")
    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[InterBatchComparator]
  }
}
