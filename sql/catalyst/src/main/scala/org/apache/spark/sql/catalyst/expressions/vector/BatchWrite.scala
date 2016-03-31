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

import java.io.IOException
import java.nio.channels.WritableByteChannel

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

abstract class BatchWrite {
  @throws(classOf[IOException])
  def write(rb: RowBatch, out: WritableByteChannel): Unit
}

object GenerateBatchWrite extends CodeGenerator[Seq[Expression], BatchWrite] {
  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in
  override protected def bind(
    in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] = in

  override protected def create(in: Seq[Expression]): BatchWrite = {
    val ctx = newCodeGenContext()
    val schema = in.map(_.dataType)

    val columnsWrite = schema.zipWithIndex.map { case (dt, idx) =>
      dt match {
        case IntegerType =>
          s"rb.columns[$idx].writeIntCVToStream(out, rb.sorted, rb.startIdx, rb.numRows);"
        case LongType =>
          s"rb.columns[$idx].writeLongCVToStream(out, rb.sorted, rb.startIdx, rb.numRows);"
        case DoubleType =>
          s"rb.columns[$idx].writeDoubleCVToStream(out, rb.sorted, rb.startIdx, rb.numRows);"
        case StringType =>
          s"rb.columns[$idx].writeStringCVToStream(out, rb.sorted, rb.startIdx, rb.numRows);"
        case _ =>
          "Not implemented yet"
      }
    }.mkString("\n")

    val code =
      s"""
      public java.lang.Object generate($exprType[] exprs) {
        return new SpecificBatchWrite(exprs);
      }

      class SpecificBatchWrite extends ${classOf[BatchWrite].getName} {
        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificBatchWrite($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        public void write(RowBatch rb, java.nio.channels.WritableByteChannel out) throws java.io.IOException {
          $columnsWrite
        }
      }
    """

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[BatchWrite]
  }
}
