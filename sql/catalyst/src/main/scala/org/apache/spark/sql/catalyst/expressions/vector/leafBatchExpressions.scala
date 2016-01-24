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
    s"ColumnVector ${ev.value} = ${ctx.INPUT_ROWBATCH}.columns[$ordinal];"
  }
}

case class BatchLiteral (underlyingExpr: Literal) extends LeafBatchExpression {
  override def eval(input: RowBatch): ColumnVector = {
    val cv = new ColumnVector(input.capacity, dataType)
    cv.isRepeating = true
    if (!cv.noNulls) {
      cv.putNull(0)
    } else {
      cv.put(0, underlyingExpr.value)
    }
    cv
  }

  override protected def genCode(
    ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val value = dataType match {
      case IntegerType => if (underlyingExpr.value == null) "0" else s"${underlyingExpr.value}"
      case LongType => if (underlyingExpr.value == null) "0" else s"${underlyingExpr.value}"
      case DoubleType => if (underlyingExpr.value == null) "0" else s"${underlyingExpr.value}"
      case StringType => if (underlyingExpr.value == null) {
          "null"
        } else {
          s"""UTF8String.fromString("${underlyingExpr.value}")"""
        }
    }

    val v = if (!underlyingExpr.nullable) {
      s"""
        ${ev.value}.${ctx.vectorName(dataType)}[0] = $value;
        ${ev.value}.isNull[0] = false;
      """
    } else {
      s"""
        ${ev.value}.${ctx.vectorName(dataType)}[0] =
          ColumnVector.${ctx.javaType(dataType)}NullValue;
        ${ev.value}.isNull[0] = true;
      """
    }
    s"""
      ColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};
      ${ev.value}.isRepeating = true;
      ${ev.value}.noNulls = ${!underlyingExpr.nullable};
      $v
    """
  }
}
