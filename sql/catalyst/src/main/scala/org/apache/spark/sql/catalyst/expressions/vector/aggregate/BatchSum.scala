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

package org.apache.spark.sql.catalyst.expressions.vector.aggregate

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedBatchExpressionCode, CodeGenContext}
import org.apache.spark.sql.catalyst.expressions.vector.{BatchCast, BatchExpression}
import org.apache.spark.sql.types.DoubleType

abstract class BatchAggregate {
  /**
    * Returns the string representation of this expression that is safe to be put in
    * code comments of generated code.
    */
  protected def toCommentSafeString: String = this.toString
    .replace("*/", "\\*\\/")
    .replace("\\u", "\\\\u")

  /**
    * Returns an [[GeneratedBatchExpressionCode]], which contains Java source code that
    * can be used to generate the result of evaluating the expression on an input row.
    *
    * @param ctx a [[CodeGenContext]]
    * @return [[GeneratedBatchExpressionCode]]
    */
  def gen(ctx: CodeGenContext): GeneratedBatchExpressionCode = {
    val primitive = ctx.freshName("primitive")
    val ve = GeneratedBatchExpressionCode("", primitive)
    ve.code = genCode(ctx, ve)
    // Add `this` in the comment.
    ve.copy(s"/* ${this.toCommentSafeString} */\n" + ve.code.trim)
  }

  /**
    * Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodeGenContext]]
    * @param ev an [[GeneratedBatchExpressionCode]] with unique terms.
    * @return Java source code
    */
  protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String
}

case class BatchCount(
    children: Seq[BatchExpression],
    underlyingFunction: AggregateFunction,
    bufferOffset: Int,
    noGroupingExpr: Boolean) extends BatchAggregate {

  assert(children.size == 1, "only support single column count for now")

  val child = children(0)

  protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.gen(ctx)

    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val tmpCount = ctx.freshName("tmpCount")

    val bufferUpdate: String = if (noGroupingExpr) {
      s"""
      if (${eval.value}.noNulls) {
        ${ctx.BUFFERS}[0].setLong($bufferOffset,
          ${ctx.BUFFERS}[0].getLong($bufferOffset) + (long) $batchSize);
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else {
        long $tmpCount = 0L;
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              $tmpCount += 1L;
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              $tmpCount += 1L;
            }
          }
        }
        ${ctx.BUFFERS}[0].setLong($bufferOffset,
          ${ctx.BUFFERS}[0].getLong($bufferOffset) + (long) $tmpCount);
      }
      """
    } else {
      s"""
      if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.BUFFERS}[i].setLong($bufferOffset, ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.BUFFERS}[i].setLong($bufferOffset, ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
          }
        }
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else if (${eval.value}.noNulls) { // not repeating & no nulls
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.BUFFERS}[i].setLong($bufferOffset, ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.BUFFERS}[i].setLong($bufferOffset, ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              ${ctx.BUFFERS}[i].setLong($bufferOffset,
                ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              ${ctx.BUFFERS}[i].setLong($bufferOffset,
                ${ctx.BUFFERS}[i].getLong($bufferOffset) + 1L);
            }
          }
        }
      }
    """
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      $bufferUpdate
    """
  }
}

case class BatchAverage(
    child: BatchExpression,
    underlyingFunction: AggregateFunction,
    bufferOffset: Int,
    noGroupingExpr: Boolean) extends BatchAggregate {

  private val dataType = DoubleType

  private val castedChild = child.dataType match {
    case DoubleType => child
    case _ => BatchCast(child, Cast(child.underlyingExpr, DoubleType))
  }

  protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = castedChild.gen(ctx)
    val countOffset = bufferOffset + 1

    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val childV = ctx.freshName("childV")
    val tmpSum = ctx.freshName("tmpSum")
    val tmpCount = ctx.freshName("tmpCount")

    val bufferUpdate: String = if (noGroupingExpr) {
      s"""
      if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
        final ${ctx.javaType(dataType)} value = $childV[0];
        ${ctx.javaType(dataType)} $tmpSum = $batchSize * value;
        ${ctx.BUFFERS}[0].setDouble($bufferOffset,
          ${ctx.BUFFERS}[0].getDouble($bufferOffset) + $tmpSum);
        ${ctx.BUFFERS}[0].setLong($countOffset,
          ${ctx.BUFFERS}[0].getLong($countOffset) + (long) $batchSize);
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else if (${eval.value}.noNulls) { // not repeating & no nulls
        ${ctx.javaType(dataType)} $tmpSum = 0;
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $tmpSum += $childV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $tmpSum += $childV[i];
          }
        }
        ${ctx.BUFFERS}[0].setDouble($bufferOffset,
          ${ctx.BUFFERS}[0].getDouble($bufferOffset) + $tmpSum);
        ${ctx.BUFFERS}[0].setLong($countOffset,
          ${ctx.BUFFERS}[0].getLong($countOffset) + (long) $batchSize);
      } else {
        ${ctx.javaType(dataType)} $tmpSum = 0;
        long $tmpCount = 0L;
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              $tmpSum += $childV[i];
              $tmpCount += 1;
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              $tmpSum += $childV[i];
              $tmpCount += 1;
            }
          }
        }
        ${ctx.BUFFERS}[0].setDouble($bufferOffset,
          ${ctx.BUFFERS}[0].getDouble($bufferOffset) + $tmpSum);
        ${ctx.BUFFERS}[0].setLong($countOffset,
          ${ctx.BUFFERS}[0].getLong($countOffset) + $tmpCount);
      }
      """
    } else {
      s"""
      if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
        final ${ctx.javaType(dataType)} value = $childV[0];
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.BUFFERS}[i].setDouble($bufferOffset,
              ${ctx.BUFFERS}[i].getDouble($bufferOffset) + value);
            ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.BUFFERS}[i].setDouble($bufferOffset,
              ${ctx.BUFFERS}[i].getDouble($bufferOffset) + value);
            ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
          }
        }
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else if (${eval.value}.noNulls) { // not repeating & no nulls
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.BUFFERS}[i].setDouble($bufferOffset,
              ${ctx.BUFFERS}[i].getDouble($bufferOffset) + $childV[i]);
            ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.BUFFERS}[i].setDouble($bufferOffset,
              ${ctx.BUFFERS}[i].getDouble($bufferOffset) + $childV[i]);
            ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              ${ctx.BUFFERS}[i].setDouble($bufferOffset,
                ${ctx.BUFFERS}[i].getDouble($bufferOffset) + $childV[i]);
              ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              ${ctx.BUFFERS}[i].setDouble($bufferOffset,
                ${ctx.BUFFERS}[i].getDouble($bufferOffset) + $childV[i]);
              ${ctx.BUFFERS}[i].setLong($countOffset, ${ctx.BUFFERS}[i].getLong($countOffset) + 1L);
            }
          }
        }
      }
      """
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      ${ctx.vectorArrayType(dataType)} $childV = ${eval.value}.${ctx.vectorName(dataType)};
      $bufferUpdate
    """
  }
}

case class BatchSum(
    child: BatchExpression,
    underlyingFunction: AggregateFunction,
    bufferOffset: Int,
    noGroupingExpr: Boolean) extends BatchAggregate {

  val dataType = child.dataType

  protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.gen(ctx)

    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val childV = ctx.freshName("childV")
    val tmpSum = ctx.freshName("tmpSum")

    val bufferUpdate: String = if (noGroupingExpr) {
      s"""
      if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
        final ${ctx.javaType(dataType)} value = $childV[0];
        ${ctx.javaType(dataType)} $tmpSum = $batchSize * value;
        ${ctx.javaType(dataType)} v =
          ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
        v = ${ctx.BUFFERS}[0].isNullAt($bufferOffset) ? $tmpSum : $tmpSum + v;
        ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, "v")};
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else if (${eval.value}.noNulls) { // not repeating & no nulls
        ${ctx.javaType(dataType)} $tmpSum = 0;
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            $tmpSum += $childV[i];
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            $tmpSum += $childV[i];
          }
        }
        ${ctx.javaType(dataType)} v =
          ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
        v = ${ctx.BUFFERS}[0].isNullAt($bufferOffset) ? $tmpSum : $tmpSum + v;
        ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, "v")};
      } else {
        ${ctx.javaType(dataType)} $tmpSum = 0;
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              $tmpSum += $childV[i];
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              $tmpSum += $childV[i];
            }
          }
        }
        ${ctx.javaType(dataType)} v =
          ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
        v = ${ctx.BUFFERS}[0].isNullAt($bufferOffset) ? $tmpSum : $tmpSum + v;
        ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, "v")};
      }
      """
    } else {
      s"""
      if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
        final ${ctx.javaType(dataType)} value = $childV[0];
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.javaType(dataType)} v =
              ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
            v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? value : value + v;
            ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.javaType(dataType)} v =
              ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
            v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? value : value + v;
            ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
          }
        }
      } else if (${eval.value}.isRepeating) { // repeating & null
        // do nothing here since it's all null
      } else if (${eval.value}.noNulls) { // not repeating & no nulls
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            ${ctx.javaType(dataType)} v =
              ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
            v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? $childV[i] : $childV[i] + v;
            ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            ${ctx.javaType(dataType)} v =
              ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
            v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? $childV[i] : $childV[i] + v;
            ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
          }
        }
      } else {
        if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
          for (int j = 0; j < $batchSize; j ++) {
            int i = $sel[j];
            if (!${eval.value}.isNull[i]) {
              ${ctx.javaType(dataType)} v =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? $childV[i] : $childV[i] + v;
              ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
            }
          }
        } else {
          for (int i = 0; i < $batchSize; i ++) {
            if (!${eval.value}.isNull[i]) {
              ${ctx.javaType(dataType)} v =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              v = ${ctx.BUFFERS}[i].isNullAt($bufferOffset) ? $childV[i] : $childV[i] + v;
              ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "v")};
            }
          }
        }
      }
    """
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      ${ctx.vectorArrayType(dataType)} $childV = ${eval.value}.${ctx.vectorName(dataType)};
      $bufferUpdate
    """
  }
}

case class BatchMax(
    child: BatchExpression,
    underlyingFunction: AggregateFunction,
    bufferOffset: Int,
    noGroupingExpr: Boolean) extends BatchAggregate {

  val dataType = child.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.gen(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val childV = ctx.freshName("childV")
    val tmpMax = ctx.freshName("tmpMax")
    val hasNotNull = ctx.freshName("hasNotNull")

    val bufferUpdate: String = if (noGroupingExpr) {
      s"""
        if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
          ${ctx.javaType(dataType)} value = $childV[0];
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, "value", "cur")})) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, "value")};
          }
        } else if (${eval.value}.isRepeating) { // repeating & null
          // do nothing here since it's all null
        } else if (${eval.value}.noNulls) { // not repeating & no nulls
          ${ctx.javaType(dataType)} $tmpMax = ${ctx.defaultValue(dataType)};
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (j == 0) {
                $tmpMax = $childV[i];
              } else {
                $tmpMax =
                  (${ctx.genGreater(dataType, s"$childV[i]", s"$tmpMax")}) ? $childV[i] : $tmpMax;
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (i == 0) {
                $tmpMax = $childV[i];
              } else {
                $tmpMax =
                  (${ctx.genGreater(dataType, s"$childV[i]", s"$tmpMax")}) ? $childV[i] : $tmpMax;
              }
            }
          }
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, s"$tmpMax", "cur")})) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, s"$tmpMax")};
          }
        } else { // not repeating && nullable
          boolean $hasNotNull = false;
          ${ctx.javaType(dataType)} $tmpMax = ${ctx.defaultValue(dataType)};
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (!${eval.value}.isNull[i]) {
                if (!$hasNotNull) {
                  $tmpMax = $childV[i];
                } else {
                  $tmpMax =
                    (${ctx.genGreater(dataType, s"$childV[i]", s"$tmpMax")}) ? $childV[i] : $tmpMax;
                }
                $hasNotNull = true;
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (!${eval.value}.isNull[i]) {
                if (!$hasNotNull) {
                  $tmpMax = $childV[i];
                } else {
                  $tmpMax =
                    (${ctx.genGreater(dataType, s"$childV[i]", s"$tmpMax")}) ? $childV[i] : $tmpMax;
                }
                $hasNotNull = true;
              }
            }
          }
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if ($hasNotNull && (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, s"$tmpMax", "cur")}))) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, s"$tmpMax")};
          }
        }
      """
    } else {
      s"""
        if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
          final ${ctx.javaType(dataType)} value = $childV[0];
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "value", "cur")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "value", "cur")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          }
        } else if (${eval.value}.isRepeating) {
          // repeating && null, do nothing here
        } else if (${eval.value}.noNulls) { // not repeating & no nulls
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              ${ctx.javaType(dataType)} value = $childV[i];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "value", "cur")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              ${ctx.javaType(dataType)} value = $childV[i];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "value", "cur")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          }
        } else { // not repeating & has null
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (!${eval.value}.isNull[i]) {
                ${ctx.javaType(dataType)} value = $childV[i];
                ${ctx.javaType(dataType)} cur =
                  ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
                if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                    (${ctx.genGreater(dataType, "value", "cur")})) {
                  ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
                }
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (!${eval.value}.isNull[i]) {
                ${ctx.javaType(dataType)} value = $childV[i];
                ${ctx.javaType(dataType)} cur =
                  ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
                if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                    (${ctx.genGreater(dataType, "value", "cur")})) {
                  ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
                }
              }
            }
          }
        }
      """
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      ${ctx.vectorArrayType(dataType)} $childV = ${eval.value}.${ctx.vectorName(dataType)};
      $bufferUpdate
    """
  }
}

case class BatchMin(
  child: BatchExpression,
  underlyingFunction: AggregateFunction,
  bufferOffset: Int,
  noGroupingExpr: Boolean) extends BatchAggregate {

  val dataType = child.dataType

  override def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.gen(ctx)
    val batchSize = ctx.freshName("validSize")
    val sel = ctx.freshName("sel")
    val childV = ctx.freshName("childV")
    val tmpMin = ctx.freshName("tmpMin")
    val hasNotNull = ctx.freshName("hasNotNull")

    val bufferUpdate: String = if (noGroupingExpr) {
      s"""
        if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
          ${ctx.javaType(dataType)} value = $childV[0];
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, "cur", "value")})) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, "value")};
          }
        } else if (${eval.value}.isRepeating) { // repeating & null
          // do nothing here since it's all null
        } else if (${eval.value}.noNulls) { // not repeating & no nulls
          ${ctx.javaType(dataType)} $tmpMin = ${ctx.defaultValue(dataType)};
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (j == 0) {
                $tmpMin = $childV[i];
              } else {
                $tmpMin =
                  (${ctx.genGreater(dataType, s"$tmpMin", s"$childV[i]")}) ? $childV[i] : $tmpMin;
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (i == 0) {
                $tmpMin = $childV[i];
              } else {
                $tmpMin =
                  (${ctx.genGreater(dataType, s"$tmpMin", s"$childV[i]")}) ? $childV[i] : $tmpMin;
              }
            }
          }
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, "cur", s"$tmpMin")})) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, s"$tmpMin")};
          }
        } else { // not repeating && nullable
          boolean $hasNotNull = false;
          ${ctx.javaType(dataType)} $tmpMin = ${ctx.defaultValue(dataType)};
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (!${eval.value}.isNull[i]) {
                if (!$hasNotNull) {
                  $tmpMin = $childV[i];
                } else {
                  $tmpMin =
                    (${ctx.genGreater(dataType, s"$tmpMin", s"$childV[i]")}) ? $childV[i] : $tmpMin;
                }
                $hasNotNull = true;
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (!${eval.value}.isNull[i]) {
                if (!$hasNotNull) {
                  $tmpMin = $childV[i];
                } else {
                  $tmpMin =
                    (${ctx.genGreater(dataType, s"$tmpMin", s"$childV[i]")}) ? $childV[i] : $tmpMin;
                }
                $hasNotNull = true;
              }
            }
          }
          ${ctx.javaType(dataType)} cur =
            ${ctx.getValue(s"${ctx.BUFFERS}[0]", dataType, s"$bufferOffset")};
          if ($hasNotNull && (${ctx.BUFFERS}[0].isNullAt($bufferOffset) ||
              (${ctx.genGreater(dataType, "cur", s"$tmpMin")}))) {
            ${ctx.setColumn(s"${ctx.BUFFERS}[0]", dataType, bufferOffset, s"$tmpMin")};
          }
        }
      """
    } else {
      s"""
        if (${eval.value}.isRepeating && ${eval.value}.noNulls) {
          final ${ctx.javaType(dataType)} value = $childV[0];
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "cur", "value")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "cur", "value")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          }
        } else if (${eval.value}.isRepeating) {
          // repeating && null, do nothing here
        } else if (${eval.value}.noNulls) { // not repeating & no nulls
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              ${ctx.javaType(dataType)} value = $childV[i];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "cur", "value")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              ${ctx.javaType(dataType)} value = $childV[i];
              ${ctx.javaType(dataType)} cur =
                ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
              if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                  (${ctx.genGreater(dataType, "cur", "value")})) {
                ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
              }
            }
          }
        } else { // not repeating & has null
          if (${ctx.INPUT_ROWBATCH}.selectedInUse) {
            for (int j = 0; j < $batchSize; j ++) {
              int i = $sel[j];
              if (!${eval.value}.isNull[i]) {
                ${ctx.javaType(dataType)} value = $childV[i];
                ${ctx.javaType(dataType)} cur =
                  ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
                if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                    (${ctx.genGreater(dataType, "cur", "value")})) {
                  ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
                }
              }
            }
          } else {
            for (int i = 0; i < $batchSize; i ++) {
              if (!${eval.value}.isNull[i]) {
                ${ctx.javaType(dataType)} value = $childV[i];
                ${ctx.javaType(dataType)} cur =
                  ${ctx.getValue(s"${ctx.BUFFERS}[i]", dataType, s"$bufferOffset")};
                if (${ctx.BUFFERS}[i].isNullAt($bufferOffset) ||
                    (${ctx.genGreater(dataType, "cur", "value")})) {
                  ${ctx.setColumn(s"${ctx.BUFFERS}[i]", dataType, bufferOffset, "value")};
                }
              }
            }
          }
        }
      """
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      ${ctx.vectorArrayType(dataType)} $childV = ${eval.value}.${ctx.vectorName(dataType)};
      $bufferUpdate
    """
  }
}
