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

package org.apache.spark.sql.execution.vector.aggregate

import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.expressions.vector._
import org.apache.spark.sql.catalyst.expressions.vector.aggregate._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.vector.RowBatch

abstract class VectorizedHashMap {
  def insertAll(hash: Array[Int], probe: RowBatch): Unit

  def rowIterator(): java.util.Iterator[InternalRow]

  def getInnerBatch(): RowBatch
}

class GenerateVectorizedHashMap(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    input: Seq[Attribute]) {

  val ctx = new CodeGenContext()

  val grps =
    groupingExpressions.map(BindReferences.bindReference(_, input)).map(exprToBatch)

  val aggBufferSchema = aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

  val hashMapSchema = (groupingExpressions ++ aggBufferSchema).map { expr =>
    s"org.apache.spark.sql.types.DataTypes.${expr.dataType}"
  }.mkString(", ")

  def fieldsAndCtor: String = {
    s"""
      private RowBatch hm;
      private org.apache.spark.sql.types.DataType[] hmSchema = {$hashMapSchema};
      private int[] links;
      private int capacity = 1 << 16;
      private double loadFactor = 0.5;
      private int numBuckets = (int) (capacity / loadFactor);
      private int maxSteps = 2;
      private int numRows = 0;

      private RowBatch currentProbe = null;
      private int[] currentHash = null;
      private int[] slots = null;

      public SpecificHashMap() {
        this.hm = RowBatch.create(hmSchema, capacity);
        this.links = new int[numBuckets];
        java.util.Arrays.fill(links, -1);
      }
    """
  }

  def equals: String = {
    val x = grps.zipWithIndex.map { case (e: BatchBoundReference, idx: Int) =>
      val dt = e.dataType
      s"""(${ctx.genEqual(dt, ctx.getCell(dt, "currentProbe", e.ordinal, "probeIdx"),
        ctx.getCell(dt, "hm", idx, "hmIdx"))})"""
    }.mkString(" && ")

    s"""
      private boolean equals(int probeIdx, int hmIdx) {
        return $x;
      }
    """
  }

  def findOrInsert: String = {
    val puts = grps.zipWithIndex.map { case (e: BatchBoundReference, idx: Int) =>
      val dt = e.dataType
      s"""
        ${ctx.putCell(dt, "hm", idx, "numRows",
          ctx.getCell(dt, "currentProbe", e.ordinal, "probeIdx"))};
      """
    }.mkString("\n")

    s"""
      private int findOrInsert(int probeIdx) {
        int step = 0;
        int idx = currentHash[probeIdx] & (numBuckets - 1);
        while (step < maxSteps) {
          if (links[idx] == -1) {
            if (numRows < capacity) {

              // initialize group by keys
              $puts

              links[idx] = numRows++;
              hm.size = numRows;
              return links[idx];
            } else {
              // No more space
              return -1;
            }
          } else if (equals(probeIdx, links[idx])) {
            return links[idx];
          }
          idx = (idx + 1) & (numBuckets - 1);
          step++;
        }
        // Didn't find it
        return -1;
      }
    """
  }

  def updateBuffers: String = {
    val aggFuncs =
      aggregateExpressions.map(BindReferences.bindReference(_, input)).map(_.aggregateFunction)

    var i = grps.size
    val batchAggFuncs = aggFuncs.map { case func =>
      func match {
        case s: Sum =>
          val x = BatchSum(exprToBatch(s.child), s, i, false)
          i += 1
          x
        case c: Count =>
          val x = BatchCount(c.children.map(exprToBatch), c, i, false)
          i += 1
          x
        case a: Average =>
          val x = BatchAverage(exprToBatch(a.child), a, i, false)
          i += 2
          x
        case ma: Max =>
          val x = BatchMax(exprToBatch(ma.child), ma, i, false)
          i += 1
          x
        case mi: Min =>
          val x = BatchMin(exprToBatch(mi.child), mi, i, false)
          i += 1
          x
        case _ => throw new NotImplementedException
      }
    }
    batchAggFuncs.map(_.genVectorized(ctx).code).mkString("\n")
  }

  def insertAll: String = {
    s"""
      public void insertAll(int[] hash, RowBatch rb) {
        this.currentProbe = rb;
        this.currentHash = hash;
        if (this.slots == null) {
          this.slots = new int[rb.capacity];
        }

        // find slots first
        if (currentProbe.selectedInUse) {
          int j = 0;
          while (j < currentProbe.size) {
            int i = currentProbe.selected[j];
            slots[i] = findOrInsert(i);
            j += 1;
          }
        } else {
          int i = 0;
          while (i < currentProbe.size) {
            slots[i] = findOrInsert(i);
            i += 1;
          }
        }

        // update buffers
        $updateBuffers
      }
    """
  }

  def rowIterator: String = {
    s"""
      public java.util.Iterator<InternalRow> rowIterator() {
        return hm.rowIterator();
      }
    """
  }

  def getInnerBatch: String = {
    s"""
      public RowBatch getInnerBatch() {
        return this.hm;
      }
    """
  }

  def generate(): VectorizedHashMap = {
    val code =
    s"""
      public java.lang.Object generate(${classOf[Expression].getName}[] exprs) {
        return new SpecificHashMap();
      }

      class SpecificHashMap extends ${classOf[VectorizedHashMap].getName} {
        $fieldsAndCtor

        $equals

        $findOrInsert

        $insertAll

        $rowIterator

        $getInnerBatch
      }
    """

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[VectorizedHashMap]
  }
}
