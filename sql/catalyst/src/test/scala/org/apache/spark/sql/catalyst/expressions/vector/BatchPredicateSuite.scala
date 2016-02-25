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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types._

class BatchPredicateSuite extends SparkFunSuite {

  test("<= < >= >") {
    val dts: Seq[DataType] = IntegerType :: DoubleType :: StringType :: Nil
    val rb = RowBatch.create(dts.toArray, 5)
    rb.columns(0).putInt(0, 1)
    rb.columns(0).putInt(1, 2)
    rb.columns(0).putInt(2, 3)
    rb.columns(0).putInt(3, 4)
    rb.columns(0).putInt(4, 5)
    rb.columns(1).putDouble(0, 0.2)
    rb.columns(1).putDouble(1, 0.5)
    rb.columns(1).putDouble(2, 0.3)
    rb.columns(1).putDouble(3, 0.4)
    rb.columns(1).putDouble(4, 0.1)
    rb.columns(2).putString(0, "1993-05-01")
    rb.columns(2).putString(1, "1994-05-01")
    rb.columns(2).putString(2, "1992-05-01")
    rb.columns(2).putString(3, "1996-05-01")
    rb.columns(2).putString(4, "1995-05-01")

    val plan = GenerateBatchPredicate.generate(
      GreaterThan(BoundReference(0, IntegerType, false), Literal(3)))
    plan.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 2)
    assert(rb.selected.take(rb.size) === 3 :: 4 :: Nil)

    rb.reset()
    val plan2 = GenerateBatchPredicate.generate(
      GreaterThan(BoundReference(2, StringType, false), Literal("1994-04-28")))
    plan2.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 3)
    assert(rb.selected.take(rb.size) === 1 :: 3 :: 4 :: Nil)

    rb.reset()
    val plan3 = GenerateBatchPredicate.generate(
      LessThanOrEqual(BoundReference(1, DoubleType, false), Literal(0.3)))
    plan3.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 3)
    assert(rb.selected.take(rb.size) === 0 :: 2 :: 4 :: Nil)
  }

  test("And") {
    val dts: Seq[DataType] = IntegerType :: DoubleType :: StringType :: Nil
    val rb = RowBatch.create(dts.toArray, 5)
    rb.columns(0).putInt(0, 1)
    rb.columns(0).putInt(1, 2)
    rb.columns(0).putInt(2, 3)
    rb.columns(0).putInt(3, 4)
    rb.columns(0).putInt(4, 5)
    rb.columns(1).putDouble(0, 0.2)
    rb.columns(1).putDouble(1, 0.5)
    rb.columns(1).putDouble(2, 0.3)
    rb.columns(1).putDouble(3, 0.4)
    rb.columns(1).putDouble(4, 0.1)
    rb.columns(2).putString(0, "1993-05-01")
    rb.columns(2).putString(1, "1994-05-01")
    rb.columns(2).putString(2, "1992-05-01")
    rb.columns(2).putString(3, "1996-05-01")
    rb.columns(2).putString(4, "1995-05-01")

    val plan = GenerateBatchPredicate.generate(
      And(GreaterThan(BoundReference(0, IntegerType, false), Literal(3)),
        LessThanOrEqual(BoundReference(1, DoubleType, false), Literal(0.3))))
    plan.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 1)
    assert(rb.selected.take(rb.size) === 4 :: Nil)
  }

  test("BatchIn") {
    val dts: Seq[DataType] = IntegerType :: StringType :: Nil
    val rb = RowBatch.create(dts.toArray, 5)
    rb.columns(0).putInt(0, 1)
    rb.columns(0).putInt(1, 2)
    rb.columns(0).putInt(2, 3)
    rb.columns(0).putInt(3, 4)
    rb.columns(0).putInt(4, 5)
    rb.columns(1).putString(0, "abc")
    rb.columns(1).putString(1, "def")
    rb.columns(1).putString(2, "hij")
    rb.columns(1).putString(3, "klm")
    rb.columns(1).putString(4, "nop")

    val plan = GenerateBatchPredicate.generate(
      In(BoundReference(0, IntegerType, false), Literal(1) :: Literal(3) :: Literal(5) :: Nil))

    plan.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 3)
    assert(rb.selected.take(rb.size) === 0 :: 2 :: 4 :: Nil)

    rb.reset()

    val plan2 = GenerateBatchPredicate.generate(
      In(BoundReference(1, StringType, false), Literal("def") :: Literal("klm"):: Nil))

    plan2.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 2)
    assert(rb.selected.take(rb.size) === 1 :: 3 :: Nil)
  }
}
