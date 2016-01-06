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
import org.apache.spark.sql.catalyst.vector._
import org.apache.spark.sql.types.{StringType, LongType, DoubleType, IntegerType}

case class BatchBoundReference(underlyingExpr: BoundReference) extends LeafBatchExpression {

  def ordinal: Int = underlyingExpr.ordinal

  override def eval(input: RowBatch): ColumnVector = input.columns(ordinal)

  override protected def genCode(
    ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    s"${ctx.cvType(dataType)} ${ev.value} = ${ctx.INPUT_ROWBATCH}.columns($ordinal);"
  }
}

case class BatchLiteral (underlyingExpr: Literal) extends LeafBatchExpression {
  override def eval(input: RowBatch): ColumnVector = {
    val cv = dataType match {
      case IntegerType => IntColumnVector(input.size)
      case LongType => LongColumnVector(input.size)
      case DoubleType => DoubleColumnVector(input.size)
      case StringType => StringColumnVector(input.size)
      case _ => throw new UnsupportedOperationException("no other type of CV supported yet")
    }
    cv.isRepeating = true
    cv.noNulls = !underlyingExpr.nullable
    if (!cv.noNulls) {
      cv.isNull(0) = true
    } else {
      cv.vector(0) = underlyingExpr.value.asInstanceOf[cv.fieldType]
    }
    cv
  }

  override protected def genCode(
    ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val value = if (!underlyingExpr.nullable) {
      s"""
        ${ev.value}.vector[0] =
          (${ctx.boxedType(dataType)}) ${underlyingExpr.value};
        ${ev.value}.isNull[0] = false;
      """
    } else {
      s"""
        ${ev.value}.vector[0] = ${ev.value}.null_value();
        ${ev.value}.isNull[0] = true;
      """
    }
    s"""
      ${ctx.cvType(dataType)} ${ev.value} = new ${ctx.cvType(dataType)}(${ctx.INPUT_ROWBATCH.size});
      ${ev.value}.isRepeating = true;
      ${ev.value}.noNulls = ${!underlyingExpr.nullable};
      $value
    """
  }
}
