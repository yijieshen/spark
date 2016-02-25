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

class BatchStringExpressionsSuite extends SparkFunSuite {

  val dts: Seq[DataType] = StringType :: Nil
  val rb = RowBatch.create(dts.toArray, 5)
  rb.columns(0).putString(0, "abcd")
  rb.columns(0).putString(1, "def")
  rb.columns(0).putString(2, "abcdef")
  rb.columns(0).putString(3, "akldm")
  rb.columns(0).putString(4, "nop")

  test("String Comparison - startsWith/endsWith/contains") {
    rb.reset()
    val plan = GenerateBatchPredicate.generate(
      StartsWith(BoundReference(0, StringType, false), Literal("abc")))
    plan.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 2)
    assert(rb.selected.take(rb.size) === 0 :: 2 :: Nil)

    rb.reset()
    val plan2 = GenerateBatchPredicate.generate(
      EndsWith(BoundReference(0, StringType, false), Literal("ef")))
    plan2.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 2)
    assert(rb.selected.take(rb.size) === 1 :: 2 :: Nil)

    rb.reset()
    val plan3 = GenerateBatchPredicate.generate(
      Contains(BoundReference(0, StringType, false), Literal("d")))
    plan3.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 4)
    assert(rb.selected.take(rb.size) === 0 :: 1 :: 2 :: 3 :: Nil)
  }

  test("like") {
    rb.reset()
    val plan = GenerateBatchPredicate.generate(
      Like(BoundReference(0, StringType, false), Literal("%a%d%")))
    plan.eval(rb)
    assert(rb.capacity === 5)
    assert(rb.size === 3)
    assert(rb.selected.take(rb.size) === 0 :: 2 :: 3 :: Nil)
  }

  test("Substring") {
    rb.reset()
    val plan = GenerateBatchProjection.generate(
      Substring(BoundReference(0, StringType, false), Literal(0), Literal(4)) :: Nil, false)
    val nrb = plan.apply(rb)
    assert(nrb.size === 5)
    assert(nrb.columns(0).getString(0).toString === "abcd")
    assert(nrb.columns(0).getString(1).toString === "def")
    assert(nrb.columns(0).getString(2).toString === "abcd")
    assert(nrb.columns(0).getString(3).toString === "akld")
    assert(nrb.columns(0).getString(4).toString === "nop")
  }
}
