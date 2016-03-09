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

import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

abstract class UnsafeRowVectorConverter {
  def apply(input: RowBatch): Array[UnsafeRow]
}

object GenerateUnsafeRowVectorConverter
    extends CodeGenerator[Seq[Expression], UnsafeRowVectorConverter] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))


  protected def create(expressions: Seq[Expression]): UnsafeRowVectorConverter = {
    val ctx = newCodeGenContext()
    val batchExpressions = expressions.map(exprToBatch)
    val numFields = expressions.size
    val numVarFields = expressions.map(_.dataType).filterNot(UnsafeRow.isFixedLength(_)).size

    val vectorGenClass = classOf[UnsafeRowVectorWriter].getName

    val vectorGen = ctx.freshName("vectorGen")
    ctx.addMutableState(vectorGenClass, vectorGen, s"this.$vectorGen = " +
      s"new $vectorGenClass(${RowBatch.DEFAULT_SIZE}, $numFields, $numVarFields);")

    val exprEvals = ctx.generateBatchExpressions(batchExpressions, false)

    val evals = batchExpressions.zip(exprEvals).zipWithIndex.map { case ((expr, eval), index) =>
      val write = expr.dataType match {
        case IntegerType => s"$vectorGen.writeColumnInteger($index, ${eval.value});"
        case LongType => s"$vectorGen.writeColumnLong($index, ${eval.value});"
        case DoubleType => s"$vectorGen.writeColumnDouble($index, ${eval.value});"
        case StringType => s"$vectorGen.writeColumnUTF8String($index, ${eval.value});"
        case _ => "Not Implemented"
      }
      s"""
        ${eval.code}
        $write
      """
    }.mkString("\n")

    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchKeyWrapper(exprs);
      }

      class SpecificBatchKeyWrapper extends ${classOf[UnsafeRowVectorConverter].getName} {
        private $exprType[] expressions;

        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchKeyWrapper($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public java.lang.Object apply(java.lang.Object rowBatch) {
          return apply((RowBatch) rowBatch);
        }

        public UnsafeRow[] apply(RowBatch ${ctx.INPUT_ROWBATCH}) {
          $vectorGen.reset(${ctx.INPUT_ROWBATCH});
          $evals
          return $vectorGen.evaluate();
        }
      }
    """
    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[UnsafeRowVectorConverter]
  }
}
