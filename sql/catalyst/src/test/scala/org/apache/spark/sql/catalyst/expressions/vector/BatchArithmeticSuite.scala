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
import org.apache.spark.sql.types.{DataType, LongType}

class BatchArithmeticSuite extends SparkFunSuite {

  val dts: Seq[DataType] = LongType :: Nil
  val rb = RowBatch.create(dts.toArray, 5)
  rb.columns(0).putLong(0, 1028)
  rb.columns(0).putLong(1, 1029)
  rb.columns(0).putLong(2, 1030)
  rb.columns(0).putLong(3, 1031)
  rb.columns(0).putLong(4, 1032)

  val row1 = new GenericMutableRow(1); row1.setLong(0, 1028)
  val row2 = new GenericMutableRow(1); row2.setLong(0, 1029)
  val row3 = new GenericMutableRow(1); row3.setLong(0, 1030)
  val row4 = new GenericMutableRow(1); row4.setLong(0, 1031)
  val row5 = new GenericMutableRow(1); row5.setLong(0, 1032)

  test("case when") {
    rb.reset()
    val plan = GenerateBatchProjection.generate(
      Pmod(new Murmur3Hash(BoundReference(0, LongType, false) :: Nil),
        Literal(5)) :: Nil, false)

    val planB = UnsafeProjection.create(
      Pmod(new Murmur3Hash(BoundReference(0, LongType, false) :: Nil),
      Literal(5)) :: Nil)

    val nrb = plan.apply(rb)
    assert(nrb.size === 5)
    assert(nrb.columns(0).intVector(0) === planB(row1).getInt(0))
    assert(nrb.columns(0).intVector(1) === planB(row2).getInt(0))
    assert(nrb.columns(0).intVector(2) === planB(row3).getInt(0))
    assert(nrb.columns(0).intVector(3) === planB(row4).getInt(0))
    assert(nrb.columns(0).intVector(4) === planB(row5).getInt(0))
  }
}
