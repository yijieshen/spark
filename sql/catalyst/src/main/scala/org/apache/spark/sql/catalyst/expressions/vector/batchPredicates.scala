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
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._


abstract class BinaryBatchComparison extends BinaryBatchOperator {

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val n = ctx.freshName("n")
    val newSize = ctx.freshName("newSize")
    val sel = ctx.freshName("sel")
    val leftV = ctx.freshName("leftV")
    val rightV = ctx.freshName("rightV")

    if (ctx.isPrimitiveType(left.dataType)
        && left.dataType != BooleanType
        && left.dataType != FloatType
        && left.dataType != DoubleType) {
      eval1.code + eval2.code + s"""
      int $n = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      ${ctx.javaType(left.dataType)}[] $leftV = ${eval1.value}.vector;
      ${ctx.javaType(right.dataType)}[] $rightV = ${eval2.value}.vector;

      // filter rows with NULL on left input
      int $newSize;
      newSize = $nu.filterNulls($leftV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      newSize = $nu.filterNulls($rightV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      // All rows with nulls has been filtered out, so just do normal filter for no-null case
      if ($n != 0 && $leftV.isRepeating && $rightV.isRepeating) {
        if (!($leftV[0] $symbol $rightV[0])) {
          ${ctx.INPUT_ROWBATCH}.size = 0;
        }
      } else if ($leftV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if ($leftV[0] $symbol $rightV[i]) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if ($leftV[0] $symbol $rightV[i]) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if ($rightV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if ($leftV[i] $symbol $rightV[0]) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if ($leftV[i] $symbol $rightV[0]) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        $newSize = 0;
        for (int j = 0; j < $n; j ++) {
          int i = $sel[j];
          if ($leftV[i] $symbol $rightV[i]) {
            $sel[$newSize ++] = i;
          }
        }
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
      } else {
        $newSize = 0;
        for (int i = 0; i < $n; i ++) {
          if ($leftV[i] $symbol $rightV[i]) {
            $sel[$newSize ++] = i;
          }
        }
        if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
          ${ctx.INPUT_ROWBATCH} = $newSize;
          ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
        }
      }
    """
    } else {
      eval1.code + eval2.code + s"""
      int $n = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      ${ctx.javaType(left.dataType)}[] $leftV = ${eval1.value}.vector;
      ${ctx.javaType(right.dataType)}[] $rightV = ${eval2.value}.vector;

      // filter rows with NULL on left input
      int $newSize;
      newSize = $nu.filterNulls($leftV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      newSize = $nu.filterNulls($rightV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      // All rows with nulls has been filtered out, so just do normal filter for no-null case
      if ($n != 0 && $leftV.isRepeating && $rightV.isRepeating) {
        if (!(${ctx.genComp(left.dataType, s"$leftV[0]", s"$rightV[0]")}) $symbol 0) {
          ${ctx.INPUT_ROWBATCH}.size = 0;
        }
      } else if ($leftV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if (${ctx.genComp(left.dataType, s"$leftV[0]", s"$rightV[i]")} $symbol 0) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genComp(left.dataType, s"$leftV[0]", s"$rightV[i]")} $symbol 0) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if ($rightV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if (${ctx.genComp(left.dataType, s"$leftV[i]", s"$rightV[0]")} $symbol 0) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genComp(left.dataType, s"$leftV[i]", s"$rightV[0]")} $symbol 0) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        $newSize = 0;
        for (int j = 0; j < $n; j ++) {
          int i = $sel[j];
          if (${ctx.genComp(left.dataType, s"$leftV[i]", s"$rightV[i]")} $symbol 0) {
            $sel[$newSize ++] = i;
          }
        }
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
      } else {
        $newSize = 0;
        for (int i = 0; i < $n; i ++) {
          if (${ctx.genComp(left.dataType, s"$leftV[i]", s"$rightV[i]")} $symbol 0) {
            $sel[$newSize ++] = i;
          }
        }
        if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
          ${ctx.INPUT_ROWBATCH} = $newSize;
          ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
        }
      }
    """
    }
  }
}

case class BatchEqualTo(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {

  override def inputType: AbstractDataType = AnyDataType

  override def symbol: String = "="

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val nu = NullUtils.getClass.getName.stripSuffix("$")

    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val n = ctx.freshName("n")
    val newSize = ctx.freshName("newSize")
    val sel = ctx.freshName("sel")
    val leftV = ctx.freshName("leftV")
    val rightV = ctx.freshName("rightV")
    eval1.code + eval2.code + s"""
      int $n = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;

      ${ctx.javaType(left.dataType)}[] $leftV = ${eval1.value}.vector;
      ${ctx.javaType(right.dataType)}[] $rightV = ${eval2.value}.vector;

      // filter rows with NULL on left input
      int $newSize;
      newSize = $nu.filterNulls($leftV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      newSize = $nu.filterNulls($rightV, ${ctx.INPUT_ROWBATCH}.selectedInUse, $sel, $n);
      if ($newSize < $n) {
        $n = ${ctx.INPUT_ROWBATCH}.size = $newSize;
        ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
      }

      // All rows with nulls has been filtered out, so just do normal filter for no-null case
      if ($n != 0 && $leftV.isRepeating && $rightV.isRepeating) {
        if (!${ctx.genEqual(left.dataType, s"$leftV[0]", s"$rightV[0]")}) {
          ${ctx.INPUT_ROWBATCH}.size = 0;
        }
      } else if ($leftV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if (${ctx.genEqual(left.dataType, s"$leftV[0]", s"$rightV[i]")}) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genEqual(left.dataType, s"$leftV[0]", s"$rightV[i]")}) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if ($rightV.isRepeating) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          $newSize = 0;
          for (int j = 0; j < $n; j ++) {
            int i = $sel[j];
            if (${ctx.genEqual(left.dataType, s"$leftV[i]", s"$rightV[0]")}) {
              $sel[$newSize ++] = i;
            }
          }
          ${ctx.INPUT_ROWBATCH}.size = $newSize;
        } else {
          $newSize = 0;
          for (int i = 0; i < $n; i ++) {
            if (${ctx.genEqual(left.dataType, s"$leftV[i]", s"$rightV[0]")}) {
              $sel[$newSize ++] = i;
            }
          }
          if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
            ${ctx.INPUT_ROWBATCH} = $newSize;
            ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
          }
        }
      } else if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
        $newSize = 0;
        for (int j = 0; j < $n; j ++) {
          int i = $sel[j];
          if (${ctx.genEqual(left.dataType, s"$leftV[i]", s"$rightV[i]")}) {
            $sel[$newSize ++] = i;
          }
        }
        ${ctx.INPUT_ROWBATCH}.size = $newSize;
      } else {
        $newSize = 0;
        for (int i = 0; i < $n; i ++) {
          if (${ctx.genEqual(left.dataType, s"$leftV[i]", s"$rightV[i]")}) {
            $sel[$newSize ++] = i;
          }
        }
        if ($newSize < ${ctx.INPUT_ROWBATCH}.size) {
          ${ctx.INPUT_ROWBATCH} = $newSize;
          ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
        }
      }
    """
  }
}

case class BatchLessThan(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = "<"
}

case class BatchLessThanOrEqual(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = "<="
}

case class BatchGreaterThan(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = ">"
}

case class BatchGreaterThanOrEqual(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = TypeCollection.Ordered
  override def symbol: String = ">="
}

case class BatchAnd(
    left: BatchExpression,
    right: BatchExpression,
    underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = BooleanType
  override def symbol: String = "&&"

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = left.gen(ctx)
    eval1.code + eval2.code
  }
}

case class BatchOr(
  left: BatchExpression,
  right: BatchExpression,
  underlyingExpr: Expression) extends BinaryBatchComparison {
  override def inputType: AbstractDataType = BooleanType
  override def symbol: String = "&&"

  // TODO: reuse int array?
  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val initialSelectedInUse = ctx.freshName("initialSelectedInUse")
    val initialSize = ctx.freshName("initialSize")
    val initialSelected = ctx.freshName("initialSelected")
    val sizeAfterFirstChild = ctx.freshName("sizeAfterFirstChild")
    val selectedAfterFirstChild = ctx.freshName("selectedAfterFirstChild")
    val curSelected = ctx.freshName("curSelected")
    val tmp = ctx.freshName("tmp")
    val unselected = ctx.freshName("unselected")
    val unselectedSize = ctx.freshName("unselectedSize")
    val newSize = ctx.freshName("newSize")
    val k = ctx.freshName("k")
    s"""
      int $initialSize = ${ctx.INPUT_ROWBATCH}.size;
      boolean $initialSelectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      int $curSelected = ${ctx.INPUT_ROWBATCH}.selected;
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

      ${eval1.code.trim}

      // Preserve the selected reference and size values generated
      // after the first child is evaluated.
      int $sizeAfterFirstChild = ${ctx.INPUT_ROWBATCH}.size;
      int[] $selectedAfterFirstChild = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $tmp = new int[${RowBatch.DEFAULT_SIZE}];
      int[] $unselected = new int[${RowBatch.DEFAULT_SIZE}];

      // calculate unselected ones in last evaluate.
      for (int j = 0; j < $initialSize; j ++) {
        $tmp[$initialSelected[j]] = 0;
      }
      for (int j = 0; j < $sizeAfterFirstChild; j ++) {
        $tmp[$selectedAfterFirstChild[j]] = 1;
      }
      int $unselectedSize = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 0) {
          $unselected[$unselectedSize ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.selected = $unselected;
      ${ctx.INPUT_ROWBATCH}.size = $unselectedSize;

      ${eval2.code.trim}

      // Merge the results
      int $newSize = ${ctx.INPUT_ROWBATCH}.size + $sizeAfterFirstChild;
      for (int i = 0; i < ${ctx.INPUT_ROWBATCH}.size; i ++) {
        $tmp[${ctx.INPUT_ROWBATCH}.selected[i]] = 1;
      }
      int $k = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 1) {
          ${ctx.INPUT_ROWBATCH}.selected[$k ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.size = $newSize;
      if ($newSize == $initialSize) {
        ${ctx.INPUT_ROWBATCH}.selectedInUse = $initialSelectedInUse;
      }

    """
  }
}

case class BatchNot(
  child: BatchExpression,
  underlyingExpr: Expression) extends UnaryBatchExpression {

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.gen(ctx)
    val initialSize = ctx.freshName("initialSize")
    val initialSelected = ctx.freshName("initialSelected")
    val sizeAfterFirstChild = ctx.freshName("sizeAfterFirstChild")
    val selectedAfterFirstChild = ctx.freshName("selectedAfterFirstChild")
    val curSelected = ctx.freshName("curSelected")
    val tmp = ctx.freshName("tmp")
    val unselected = ctx.freshName("unselected")
    val unselectedSize = ctx.freshName("unselectedSize")
    s"""
      int $initialSize = ${ctx.INPUT_ROWBATCH}.size;
      int $curSelected = ${ctx.INPUT_ROWBATCH}.selected;
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

      ${eval.code.trim}

      // Preserve the selected reference and size values generated
      // after the first child is evaluated.
      int $sizeAfterFirstChild = ${ctx.INPUT_ROWBATCH}.size;
      int[] $selectedAfterFirstChild = ${ctx.INPUT_ROWBATCH}.selected;
      int[] $tmp = new int[${RowBatch.DEFAULT_SIZE}];
      int[] $unselected = new int[${RowBatch.DEFAULT_SIZE}];

      // calculate unselected ones in last evaluate.
      for (int j = 0; j < $initialSize; j ++) {
        $tmp[$initialSelected[j]] = 0;
      }
      for (int j = 0; j < $sizeAfterFirstChild; j ++) {
        $tmp[$selectedAfterFirstChild[j]] = 1;
      }
      int $unselectedSize = 0;
      for (int j = 0; j < $initialSize; j ++) {
        int i = $initialSelected[j];
        if ($tmp[i] == 0) {
          $unselected[$unselectedSize ++] = i;
        }
      }

      ${ctx.INPUT_ROWBATCH}.selected = $unselected;
      ${ctx.INPUT_ROWBATCH}.size = $unselectedSize;
      ${ctx.INPUT_ROWBATCH}.selectedInUse = true;
    """
  }
}
