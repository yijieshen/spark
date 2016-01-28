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
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}
import org.apache.spark.sql.types.DataType

case class BatchIf(
    predicate: BatchExpression,
    trueValue: BatchExpression,
    falseValue: BatchExpression,
    underlyingExpr: Expression) extends BatchExpression {

  override def children: Seq[BatchExpression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def dataType: DataType = underlyingExpr.dataType

  override def eval(input: RowBatch): ColumnVector = {
    null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val condEval = predicate.gen(ctx)
    val trueEval = trueValue.gen(ctx)
    val falseEval = falseValue.gen(ctx)

    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val initialSize = ctx.freshName("initialSize")
    val initialSelectedInUse = ctx.freshName("initialSelectedInUse")
    val curSelected = ctx.freshName("curSelected")
    val initialSelected = ctx.freshName("initialSelected")
    val sizeAfterCondition = ctx.freshName("sizeAfterCondition")
    val selectedAfterCondition = ctx.freshName("selectedAfterCondition")
    val tmp = ctx.freshName("tmp")
    val unselectedSize = ctx.freshName("unselectedSize")
    val unselected = ctx.freshName("unselected")

    val trueV = ctx.freshName("trueV")
    val falseV = ctx.freshName("falseV")
    val resultV = ctx.freshName("resultV")

    s"""
      ColumnVector ${ev.value} = null;

      int $initialSize = ${ctx.INPUT_ROWBATCH}.size;
      boolean $initialSelectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      int[] $curSelected = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $initialSelected = new int[${RowBatch.DEFAULT_SIZE}];
      if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        System.arraycopy($curSelected, 0, $initialSelected, 0, $initialSize);
      } else {
        for (int i = 0; i < n; i ++) {
          $initialSelected[i] = i;
          $curSelected[i] = i;
        }
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      ${condEval.code}

      //
      int $sizeAfterCondition = ${ctx.INPUT_ROWBATCH}.size;
      int[] $selectedAfterCondition = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $tmp = new int[${RowBatch.DEFAULT_SIZE}];
      int[] $unselected = new int[${RowBatch.DEFAULT_SIZE}];

      // calculate unselected ones in last evaluate.
      for (int j = 0; j < $initialSize; j ++) {
        $tmp[$initialSelected[j]] = 0;
      }
      for (int j = 0; j < $sizeAfterCondition; j ++) {
        $tmp[$selectedAfterCondition[j]] = 1;
      }
      int $unselectedSize = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 0) {
          $unselected[$unselectedSize ++] = i;
        }
      }

      // recover the rowBatch to the state before condition
      ${ctx.INPUT_ROWBATCH}.size = $initialSize;
      ${ctx.INPUT_ROWBATCH}.selected = $initialSelected;
      ${ctx.INPUT_ROWBATCH}.selectedInUse = $initialSelectedInUse;

      if ($sizeAfterCondition == $initialSize) { // all true
        ${trueEval.code}
        ${ev.value} = ${trueEval.value};
      } else if ($sizeAfterCondition == 0) { // all false
        ${falseEval.code}
        ${ev.value} = ${falseEval.value};
      } else {
        ${trueEval.code}
        ${falseEval.code}
        ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};
        ${ctx.javaType(trueValue.dataType)}[] $trueV =
          ${trueEval.value}.${ctx.vectorName(trueValue.dataType)};
        ${ctx.javaType(falseValue.dataType)}[] $falseV =
          ${falseEval.value}.${ctx.vectorName(falseValue.dataType)};
        ${ctx.javaType(dataType)}[] $resultV = ${ev.value}.${ctx.vectorName(dataType)};

        ${ev.value}.noNulls = ${trueEval.value}.noNulls && ${falseEval.value}.noNulls;

        // iterate over true values
        if (${trueEval.value}.noNulls) {
          if (${trueEval.value}.isRepeating) {
            for (int j = 0; j < $sizeAfterCondition; j ++) {
              int i = $selectedAfterCondition[j];
              $resultV[i] = $trueV[0];
            }
          } else {
            for (int j = 0; j < $sizeAfterCondition; j ++) {
              int i = $selectedAfterCondition[j];
              $resultV[i] = $trueV[i];
            }
          }
        } else { // have nulls in true expressions
          if (${trueEval.value}.isRepeating) {
            for (int j = 0; j < $sizeAfterCondition; j ++) {
              int i = $selectedAfterCondition[j];
              ${ev.value}.isNull[i] = true;
            }
          } else {
            for (int j = 0; j < $sizeAfterCondition; j ++) {
              int i = $selectedAfterCondition[j];
              ${ev.value}.isNull[i] = ${trueEval.value}.isNull[i];
              $resultV[i] = $trueV[i];
            }
          }
        }

        // iterate over false values
        if (${falseEval.value}.noNulls) {
          if (${falseEval.value}.isRepeating) {
            for (int j = 0; j < $unselectedSize; j ++) {
              int i = $unselected[j];
              $resultV[i] = $falseV[0];
            }
          } else {
            for (int j = 0; j < $unselectedSize; j ++) {
              int i = $unselected[j];
              $resultV[i] = $falseV[i];
            }
          }
        } else { // have nulls in false expressions
          if (${falseEval.value}.isRepeating) {
            for (int j = 0; j < $unselectedSize; j ++) {
              int i = $unselected[j];
              ${ev.value}.isNull[i] = true;
            }
          } else {
            for (int j = 0; j < $unselectedSize; j ++) {
              int i = $unselected[j];
              ${ev.value}.isNull[i] = ${falseEval.value}.isNull[i];
              $resultV[i] = $falseV[i];
            }
          }
        }
      }

      /* For the case when the output can have null values, follow
       * the convention that the data values must be 1 for long and
       * NaN for double. This is to prevent possible later zero-divide errors
       * in complex arithmetic expressions like col2 / (col1 - 1)
       * in the case when some col1 entries are null.
       */
      $nu.setNullDataEntries${ctx.boxedType(dataType)}(
        ${ev.value}, ${ctx.INPUT_ROWBATCH}.selectedInUse, $initialSelected, $initialSize);
    """
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"
}
