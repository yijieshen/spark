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

class BatchConditionalExpressionSuite extends SparkFunSuite {

  val dts: Seq[DataType] = StringType :: IntegerType :: Nil
  val rb = RowBatch.create(dts.toArray, 5)
  rb.columns(0).putString(0, "abcd")
  rb.columns(0).putString(1, "def")
  rb.columns(0).putString(2, "abcdef")
  rb.columns(0).putString(3, "akldm")
  rb.columns(0).putString(4, "nop")
  rb.columns(1).putInt(0, 1)
  rb.columns(1).putInt(1, 2)
  rb.columns(1).putInt(2, 3)
  rb.columns(1).putInt(3, 4)
  rb.columns(1).putInt(4, 5)

  test("case when") {
    rb.reset()
    val plan = GenerateBatchProjection.generate(
      CaseWhen(In(BoundReference(0, StringType, false), Literal("abcd") :: Literal("nop") :: Nil) ::
        BoundReference(1, IntegerType, false) :: Literal(-1) :: Nil) :: Nil, false)

    val nrb = plan.apply(rb)
    assert(nrb.size === 5)
    assert(nrb.columns(0).intVector === 1 :: -1 :: -1 :: -1 :: 5 :: Nil)
  }
}
