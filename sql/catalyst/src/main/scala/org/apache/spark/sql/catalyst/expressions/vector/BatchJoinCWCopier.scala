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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

// scalastyle:off
abstract class BatchJoinCWCopier {
  def copyLeftRun(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, length: Int): Unit
  def copyLeftRuns(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int): Unit
  def copyLeftRunsWithStep(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int, step: Int): Unit
  def copyLeftRepeat(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int): Unit
  def copyLeftRepeats(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int): Unit

  def copyRightRun(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, length: Int): Unit
  def copyRightRuns(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int): Unit
  def copyRightRunsWithStep(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int, step: Int): Unit
  def copyRightRepeat(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int): Unit
  def copyRightRepeats(from: RowBatch, fromIdx: Int, to: RowBatch, toIdx: Int, repeat: Int, length: Int): Unit

  def putLeftNulls(to: RowBatch, toIdx: Int, length: Int): Unit
  def putRightNulls(to: RowBatch, toIdx: Int, length: Int): Unit
}


object GenerateBatchJoinCWCopier extends CodeGenerator[Seq[Seq[Expression]], BatchJoinCWCopier] {

  override protected def create(
    in: Seq[Seq[Expression]]): BatchJoinCWCopier = throw new UnsupportedOperationException

  override protected def canonicalize(in: Seq[Seq[Expression]]): Seq[Seq[Expression]] = in

  override protected def bind(
    in: Seq[Seq[Expression]],
    inputSchema: Seq[Attribute]): Seq[Seq[Expression]] = in

  def generate(in: Seq[Seq[Expression]], defaultCapacity: Int): BatchJoinCWCopier = {
    val ctx = newCodeGenContext()
    ctx.setBatchCapacity(defaultCapacity)

    val leftSchema = in(0).map(_.dataType)
    val rightSchema = in(1).map(_.dataType)
    val rightOffset = in(0).size

    val leftRunCopier = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case LongType =>
          s"to.columns[$idx].putLongsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case DoubleType =>
          s"to.columns[$idx].putDoublesRun(from.columns[$idx], fromIdx, toIdx, length);"
        case StringType =>
          s"to.columns[$idx].putStringsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val leftRunsCopier = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case LongType =>
          s"to.columns[$idx].putLongsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case DoubleType =>
          s"to.columns[$idx].putDoublesRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case StringType =>
          s"to.columns[$idx].putStringsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val leftRunsWithStepCopier = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case LongType =>
          s"to.columns[$idx].putLongsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case DoubleType =>
          s"to.columns[$idx].putDoublesRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case StringType =>
          s"to.columns[$idx].putStringsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val leftRepeatCopier = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case LongType =>
          s"to.columns[$idx].putLongsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case DoubleType =>
          s"to.columns[$idx].putDoublesRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case StringType =>
          s"to.columns[$idx].putStringsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val leftRepeatsCopier = leftSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx].putIntsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case LongType =>
          s"to.columns[$idx].putLongsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case DoubleType =>
          s"to.columns[$idx].putDoublesRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case StringType =>
          s"to.columns[$idx].putStringsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val leftNullsSetter = leftSchema.zipWithIndex.map { case (dt, idx) =>
      s"to.columns[$idx].putNulls(toIdx, length);"
    }.mkString("\n")

    val rightNullsSetter = rightSchema.zipWithIndex.map { case (dt, idx) =>
      s"to.columns[$idx + $rightOffset].putNulls(toIdx, length);"
    }.mkString("\n")

    val rightRunCopier = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx + $rightOffset].putIntsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case LongType =>
          s"to.columns[$idx + $rightOffset].putLongsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case DoubleType =>
          s"to.columns[$idx + $rightOffset].putDoublesRun(from.columns[$idx], fromIdx, toIdx, length);"
        case StringType =>
          s"to.columns[$idx + $rightOffset].putStringsRun(from.columns[$idx], fromIdx, toIdx, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val rightRunsCopier = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx + $rightOffset].putIntsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case LongType =>
          s"to.columns[$idx + $rightOffset].putLongsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case DoubleType =>
          s"to.columns[$idx + $rightOffset].putDoublesRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case StringType =>
          s"to.columns[$idx + $rightOffset].putStringsRuns(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val rightRunsWithStepCopier = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx + $rightOffset].putIntsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case LongType =>
          s"to.columns[$idx + $rightOffset].putLongsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case DoubleType =>
          s"to.columns[$idx + $rightOffset].putDoublesRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case StringType =>
          s"to.columns[$idx + $rightOffset].putStringsRunsWithStep(from.columns[$idx], fromIdx, toIdx, repeat, length, step);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val rightRepeatCopier = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx + $rightOffset].putIntsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case LongType =>
          s"to.columns[$idx + $rightOffset].putLongsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case DoubleType =>
          s"to.columns[$idx + $rightOffset].putDoublesRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case StringType =>
          s"to.columns[$idx + $rightOffset].putStringsRepeat(from.columns[$idx], fromIdx, toIdx, repeat);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val rightRepeatsCopier = rightSchema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"to.columns[$idx + $rightOffset].putIntsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case LongType =>
          s"to.columns[$idx + $rightOffset].putLongsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case DoubleType =>
          s"to.columns[$idx + $rightOffset].putDoublesRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case StringType =>
          s"to.columns[$idx + $rightOffset].putStringsRepeats(from.columns[$idx], fromIdx, toIdx, repeat, length);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")


    val code = s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchJoinCWCopier(exprs);
      }

      class SpecificBatchJoinCWCopier extends ${classOf[BatchJoinCWCopier].getName} {
        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchJoinCWCopier($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public void copyLeftRun(RowBatch from, int fromIdx, RowBatch to, int toIdx, int length) {
          $leftRunCopier
        }

        public void copyRightRun(RowBatch from, int fromIdx, RowBatch to, int toIdx, int length) {
          $rightRunCopier
        }

        public void copyLeftRuns(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length) {
          $leftRunsCopier
        }

        public void copyRightRuns(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length) {
          $rightRunsCopier
        }

        public void copyLeftRunsWithStep(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length, int step) {
          $leftRunsWithStepCopier
        }

        public void copyRightRunsWithStep(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length, int step) {
          $rightRunsWithStepCopier
        }

        public void copyLeftRepeat(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat) {
          $leftRepeatCopier
        }

        public void copyRightRepeat(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat) {
          $rightRepeatCopier
        }

        public void copyLeftRepeats(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length) {
          $leftRepeatsCopier
        }

        public void copyRightRepeats(RowBatch from, int fromIdx, RowBatch to, int toIdx, int repeat, int length) {
          $rightRepeatsCopier
        }

        public void putLeftNulls(RowBatch to, int toIdx, int length) {
          $leftNullsSetter
        }

        public void putRightNulls(RowBatch to, int toIdx, int length) {
          $rightNullsSetter
        }
      }
    """

    val c = CodeGenerator.compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchJoinCWCopier]
  }
}
// scalastyle:on
