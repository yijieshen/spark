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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedBatchExpressionCode, CodeGenContext}
import org.apache.spark.sql.types._

abstract class BinaryBatchArithmetic extends BinaryBatchOperator {
  override def dataType: DataType = left.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {

    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val leftV = ctx.freshName("leftV")
    val rightV = ctx.freshName("rightV")
    val resultV = ctx.freshName("resultV")
    eval1.code + eval2.code + s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      ${ctx.javaType(left.dataType)}[] $leftV = ${eval1.value}.vector;
      ${ctx.javaType(right.dataType)}[] $rightV = ${eval2.value}.vector;
      ${ctx.cvType(dataType)} ${ev.value} = new ${ctx.cvType(dataType)}($batchSize);
      ${ctx.javaType(dataType)}[] $resultV = ${ev.value}.vector;

      ${ev.value}.isRepeating =
        ${eval1.value}.isRepeating && ${eval2.value}.isRepeating ||
        ${eval1.value}.isRepeating && !${eval1.value}.noNulls && ${eval1.value}.isNull[0] ||
        ${eval2.value}.isRepeating && !${eval2.value}.noNulls && ${eval2.value}.isNull[0];
      $nu.propagateNullsForBinaryExpression(
        ${eval1.value}, ${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse);

      /*
       * Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        $resultV[0] = $leftV[0] $symbol $rightV[0];
      } else if (${eval1.value}.isRepeating) {
        final ${ctx.javaType(left.dataType)} leftValue = $leftV[0];
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = leftValue $symbol $rightV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = leftValue $symbol $rightV[i];
          }
        }
      } else if (${eval2.value}.isRepeating) {
        final ${ctx.javaType(right.dataType)} rightValue = $rightV[0];
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = $leftV[i] $symbol rightValue;
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = $leftV[i] $symbol rightValue;
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = $leftV[i] $symbol $rightV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = $leftV[i] $symbol $rightV[i];
          }
        }
      }

      /*
       * For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      $nu.setNullDataEntries(
        ${ev.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $batchSize);

    """
  }
}

case class BatchAdd(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"
}

case class BatchSubtract(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"
}

case class BatchMultiply(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"
}

case class BatchDivide(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "/"

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {

    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val leftV = ctx.freshName("leftV")
    val rightV = ctx.freshName("rightV")
    val resultV = ctx.freshName("resultV")
    eval1.code + eval2.code + s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      ${ctx.javaType(left.dataType)}[] $leftV = ${eval1.value}.vector;
      ${ctx.javaType(right.dataType)}[] $rightV = ${eval2.value}.vector;
      ${ctx.cvType(dataType)} ${ev.value} = new ${ctx.cvType(dataType)}($batchSize);
      ${ctx.javaType(dataType)}[] $resultV = ${ev.value}.vector;

      ${ev.value}.isRepeating =
        ${eval1.value}.isRepeating && ${eval2.value}.isRepeating ||
        ${eval1.value}.isRepeating && !${eval1.value}.noNulls && ${eval1.value}.isNull[0] ||
        ${eval2.value}.isRepeating && !${eval2.value}.noNulls && ${eval2.value}.isNull[0];
      $nu.propagateNullsForBinaryExpression(
        ${eval1.value}, ${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse);
      $nu.propagateZeroDenomAsNulls(${eval2.value}, ${ev.value},
        $sel, $batchSize, ${ctx.INPUT_ROWBATCH}.selectedInUse)

      /*
       * Disregard nulls for processing. In other words,
       * the arithmetic operation is performed even if one or
       * more inputs are null. This is to improve speed by avoiding
       * conditional checks in the inner loop.
       */
      if (${eval1.value}.isRepeating && ${eval2.value}.isRepeating) {
        $resultV[0] = $leftV[0] $symbol $rightV[0];
      } else if (${eval1.value}.isRepeating) {
        final ${ctx.javaType(left.dataType)} leftValue = $leftV[0];
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = leftValue $symbol $rightV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = leftValue $symbol $rightV[i];
          }
        }
      } else if (${eval2.value}.isRepeating) {
        final ${ctx.javaType(right.dataType)} rightValue = $rightV[0];
        if (rightValue == 0) {
          // this part is handled in propagateZeroDenomAsNulls
        } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = $leftV[i] $symbol rightValue;
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = $leftV[i] $symbol rightValue;
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $resultV[i] = $leftV[i] $symbol $rightV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $resultV[i] = $leftV[i] $symbol $rightV[i];
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      $nu.setNullDataEntries(
        ${ev.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $batchSize);

    """
  }
}

