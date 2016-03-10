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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedBatchExpressionCode}
import org.apache.spark.sql.catalyst.vector.{ColumnVector, RowBatch}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32

case class BatchMurmur3Hash(
    children: Seq[BatchExpression],
    seed: Int,
    underlyingExpr: Expression) extends BatchExpression {

  def this(children: Seq[BatchExpression], underlyingExpr: Expression) =
    this(children, 42, underlyingExpr)

  override def dataType: DataType = IntegerType
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = false

  override def eval(input: RowBatch): ColumnVector = null

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedBatchExpressionCode): String = {
    val hasher = classOf[Murmur3_x86_32].getName

    val batchSize = ctx.freshName("batchSize")
    val sel = ctx.freshName("sel")
    val selectedInUse = ctx.freshName("selectedInUse")
    val resultV = ctx.freshName("resultV")

    val childrenCode = children.map { child =>
      val cur = child.gen(ctx)
      val dt = child.dataType
      val curV = ctx.freshName("curV")
      dt match {
        case StringType =>
          val bao = Platform.BYTE_ARRAY_OFFSET
          s"""
            ${cur.code}
            $curV = ${cur.value}.${ctx.vectorName(StringType)};
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[0], ${cur.value}.starts[0] + $bao, ${cur.value}.lengths[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[0], ${cur.value}.starts[0] + $bao, ${cur.value}.lengths[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[i], ${cur.value}.starts[i] + $bao, ${cur.value}.lengths[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashUnsafeBytes(
                    $curV[i], ${cur.value}.starts[i] + $bao, ${cur.value}.lengths[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashUnsafeBytes($curV[i],
                      ${cur.value}.starts[i] + $bao, ${cur.value}.lengths[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashUnsafeBytes($curV[i],
                      ${cur.value}.starts[i] + $bao, ${cur.value}.lengths[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case IntegerType =>
          s"""
            ${cur.code}
            $curV = ${cur.value}.${ctx.vectorName(IntegerType)};
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashInt($curV[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashInt($curV[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashInt($curV[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case LongType =>
          s"""
            ${cur.code}
            $curV = ${cur.value}.${ctx.vectorName(LongType)};
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong($curV[0], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong($curV[0], $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashLong($curV[i], $resultV[i]);
                  }
                }
              }
            }
          """
        case DoubleType =>
          s"""
            ${cur.code}
            $curV = ${cur.value}.${ctx.vectorName(DoubleType)};
            if (${cur.value}.noNulls && ${cur.value}.isRepeating) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[0]), $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[0]), $resultV[i]);
                }
              }
            } else if (${cur.value}.isRepeating) {
              // all nulls, should not change hash value
            } else if (${cur.value}.noNulls) {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                }
              }
            } else {
              if ($selectedInUse) {
                for (int j = 0; j < $batchSize; j ++) {
                  int i = $sel[j];
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                  }
                }
              } else {
                for (int i = 0; i < $batchSize; i ++) {
                  if (!${cur.value}.isNull[i]) {
                    $resultV[i] = $hasher.hashLong(Double.doubleToLongBits($curV[i]), $resultV[i]);
                  }
                }
              }
            }
          """
        case _ => "Not Supported yet"
      }
    }.mkString("\n")

    s"""
      int $batchSize = ${ctx.INPUT_ROWBATCH}.size;
      int[] $sel = ${ctx.INPUT_ROWBATCH}.selected;
      boolean $selectedInUse = ${ctx.INPUT_ROWBATCH}.selectedInUse;

      ColumnVector ${ev.value} = ${ctx.newVector(s"${ctx.INPUT_ROWBATCH}.capacity", dataType)};
      int[] $resultV = ${ev.value}.intVector;
      java.util.Arrays.fill($resultV, $seed);

      $childrenCode
    """
  }
}
