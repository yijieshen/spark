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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, SortDirection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.DoublePrefixComparator

case class BatchSortOrder(
    child: BatchExpression,
    direction: SortDirection,
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override def foldable: Boolean = false
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  def isAscending: Boolean = direction == Ascending
}

case class BatchSortPrefix(
    child: BatchSortOrder,
    underlyingExpr: Expression) extends UnaryBatchExpression {

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val eval = child.child.gen(ctx)

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val childV = ctx.freshName("childV")
    val resultV = ctx.freshName("resultV")
    val childStr = ctx.freshName("childStr")

    val DoublePrefixCmp = classOf[DoublePrefixComparator].getName
    val doubleMin = DoublePrefixComparator.computePrefix(Double.NegativeInfinity)

    val prefixGenCode = child.dataType match {
      case StringType =>
        s"""
          UTF8String $childStr = new UTF8String();
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              $childStr.update($childV[0], ${eval.value}.starts[0], ${eval.value}.lengths[0]);
              $resultV[0] = $childStr.getPrefix();
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                $childStr.update($childV[i], ${eval.value}.starts[i], ${eval.value}.lengths[i]);
                $resultV[i] = $childStr.getPrefix();
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                $childStr.update($childV[i], ${eval.value}.starts[i], ${eval.value}.lengths[i]);
                $resultV[i] = $childStr.getPrefix();
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              $resultV[0] = 0L;
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = 0L;
                } else {
                  $childStr.update($childV[i], ${eval.value}.starts[i], ${eval.value}.lengths[i]);
                  $resultV[i] = $childStr.getPrefix();
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = 0L;
                } else {
                  $childStr.update($childV[i], ${eval.value}.starts[i], ${eval.value}.lengths[i]);
                  $resultV[i] = $childStr.getPrefix();
                }
              }
            }
          }
        """
      case _: IntegralType =>
        s"""
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              $resultV[0] = (long) $childV[0];
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                $resultV[i] = (long) $childV[i];
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                $resultV[i] = (long) $childV[i];
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              $resultV[0] = ${Long.MinValue}L;
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = ${Long.MinValue}L;
                } else {
                  $resultV[i] = (long) $childV[i];
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = ${Long.MinValue}L;
                } else {
                  $resultV[i] = (long) $childV[i];
                }
              }
            }
          }
        """
      case FloatType | DoubleType =>
        s"""
          if (${eval.value}.noNulls) {
            if (${eval.value}.isRepeating) {
              $resultV[0] = $DoublePrefixCmp.computePrefix((double) $childV[0]);
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                $resultV[i] = $DoublePrefixCmp.computePrefix((double) $childV[i]);
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                $resultV[i] = $DoublePrefixCmp.computePrefix((double) $childV[i]);
              }
            }
          } else {
            if (${eval.value}.isRepeating) {
              $resultV[0] = $doubleMin;
              ${ev.value}.isRepeating = true;
            } else if ($selectedInUse) {
              for (int j = 0; j < $batchSize; j ++) {
                int i = $sel[j];
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = $doubleMin;
                } else {
                  $resultV[i] = $DoublePrefixCmp.computePrefix((double) $childV[i]);
                }
              }
            } else {
              for (int i = 0; i < $batchSize; i ++) {
                if (${eval.value}.isNull[i]) {
                  $resultV[i] = $doubleMin;
                } else {
                  $resultV[i] = $DoublePrefixCmp.computePrefix((double) $childV[i]);
                }
              }
            }
          }
        """
      case _ => "Not implemented yet"
    }

    s"""
      ${eval.code}
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;
      ColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", LongType)};
      ${ev.value}.noNulls = true;
      ${ctx.vectorArrayType(child.dataType)} $childV =
        ${eval.value}.${ctx.vectorName(child.dataType)};
      long[] $resultV = ${ev.value}.longVector;
      $prefixGenCode
    """
  }
}
