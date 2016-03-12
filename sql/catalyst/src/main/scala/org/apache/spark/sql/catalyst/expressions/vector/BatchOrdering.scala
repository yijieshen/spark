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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

/**
  * Inherits some default implementation for Java from `Ordering[Integer]`
  */
class BatchOrdering extends Ordering[Integer] {
  def compare(a: Integer, b: Integer): Int = {
    throw new UnsupportedOperationException
  }

  def reset(rb: RowBatch): Unit = {
    throw new UnsupportedOperationException
  }
}

object GenerateBatchOrdering extends CodeGenerator[Seq[SortOrder], BatchOrdering] {

  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  def genComparisons(ctx: CodeGenContext, ordering: Seq[SortOrder]): String = {
    ordering.map { order =>
      val eval = exprToBatch(order.child).gen(ctx)
      val asc = order.direction == Ascending
      val dt = order.child.dataType
      val pa = ctx.freshName("primitiveA")
      val pb = ctx.freshName("primitiveB")
      val childV = ctx.freshName("childV")
      dt match {
        case StringType =>
          s"""
            ${ctx.vectorArrayType(dt)} $childV = ${eval.value}.${ctx.vectorName(dt)};
            UTF8String $pa = new UTF8String();
            UTF8String $pb = new UTF8String();
            if (${eval.value}.isRepeating) {
              // Nothing
            } else if (${eval.value}.noNulls) {
              $pa.update($childV[a], ${eval.value}.starts[a], ${eval.value}.lengths[a]);
              $pb.update($childV[b], ${eval.value}.starts[b], ${eval.value}.lengths[b]);
              int comp = ${ctx.genComp(dt, pa, pb)};
              if (comp != 0) {
                return ${if (asc) "comp" else "-comp"};
              }
            } else {
              if (${eval.value}.isNull[a] && ${eval.value}.isNull[b]) {
                // Nothing
              } else if (${eval.value}.isNull[a]) {
                return ${if (asc) "-1" else "1"};
              } else if (${eval.value}.isNull[b]) {
                return ${if (asc) "1" else "-1"};
              } else {
                $pa.update($childV[a], ${eval.value}.starts[a], ${eval.value}.lengths[a]);
                $pb.update($childV[b], ${eval.value}.starts[b], ${eval.value}.lengths[b]);
                int comp = ${ctx.genComp(dt, pa, pb)};
                if (comp != 0) {
                  return ${if (asc) "comp" else "-comp"};
                }
              }
            }
          """
        case _ =>
          s"""
            ${ctx.vectorArrayType(dt)} $childV = ${eval.value}.${ctx.vectorName(dt)};
            ${ctx.javaType(dt)} $pa;
            ${ctx.javaType(dt)} $pb;
            if (${eval.value}.isRepeating) {
              // Nothing
            } else if (${eval.value}.noNulls) {
              $pa = $childV[a];
              $pb = $childV[b];
              int comp = ${ctx.genComp(dt, pa, pb)};
              if (comp != 0) {
                return ${if (asc) "comp" else "-comp"};
              }
            } else {
              if (${eval.value}.isNull[a] && ${eval.value}.isNull[b]) {
                // Nothing
              } else if (${eval.value}.isNull[a]) {
                return ${if (asc) "-1" else "1"};
              } else if (${eval.value}.isNull[b]) {
                return ${if (asc) "1" else "-1"};
              } else {
                $pa = $childV[a];
                $pb = $childV[b];
                int comp = ${ctx.genComp(dt, pa, pb)};
                if (comp != 0) {
                  return ${if (asc) "comp" else "-comp"};
                }
              }
            }
          """
      }
    }.mkString("\n")
  }

  protected def create(in: Seq[SortOrder]): BatchOrdering = {
    val ctx = newCodeGenContext()
    val comparisons = genComparisons(ctx, in)
    val code = s"""
      public SpecificBatchOrdering generate($exprType[] expr) {
        return new SpecificBatchOrdering(expr);
      }

      class SpecificBatchOrdering extends ${classOf[BatchOrdering].getName} {

        private $exprType[] expressions;
        private RowBatch current;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificOrdering($exprType[] expr) {
          expressions = expr;
          ${initMutableStates(ctx)}
        }

        public void reset(RowBatch rb) {
          this.current = rb;
        }

        public int compare(Integer a, Integer b) {
          RowBatch ${ctx.INPUT_ROWBATCH} = current;
          $comparisons
          return 0;
        }
      }
    """
    logDebug(s"Generated Ordering: ${CodeFormatter.format(code)}")
    compile(code).generate(ctx.references.toArray).asInstanceOf[BatchOrdering]
  }
}

object BatchOrderings {

  def needFurtherCompare(origin: Seq[SortOrder]): Boolean = {
    if (origin == 1) {
      origin(0).dataType match {
        case _: IntegralType | FloatType | DoubleType => false
        case _ => true
      }
    } else {
      true
    }
  }
}
