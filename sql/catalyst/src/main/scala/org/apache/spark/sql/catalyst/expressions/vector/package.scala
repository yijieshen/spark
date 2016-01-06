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

package org.apache.spark.sql.catalyst.expressions

package object vector {

  def exprToBatch(expr: Expression): BatchExpression = {
    expr match {
      case l: Literal => BatchLiteral(l)
      case b: BoundReference => BatchBoundReference(b)

      case a @ Add(l, r) => BatchAdd(exprToBatch(l), exprToBatch(r), a)
      case s @ Subtract(l, r) => BatchSubtract(exprToBatch(l), exprToBatch(r), s)
      case m @ Multiply(l, r) => BatchMultiply(exprToBatch(l), exprToBatch(r), m)
      case d @ Divide(l, r) => BatchDivide(exprToBatch(l), exprToBatch(r), d)

      case ge @ GreaterThanOrEqual(l, r) =>
        BatchGreaterThanOrEqual(exprToBatch(l), exprToBatch(r), ge)
      case gt @ GreaterThan(l, r) =>
        BatchGreaterThan(exprToBatch(l), exprToBatch(r), gt)
      case le @ LessThanOrEqual(l, r) =>
        BatchLessThanOrEqual(exprToBatch(l), exprToBatch(r), le)
      case lt @ LessThan(l, r) =>
        BatchLessThan(exprToBatch(l), exprToBatch(r), lt)

      case e @ EqualTo(l, r) => BatchEqualTo(exprToBatch(l), exprToBatch(r), e)
      case a @ And(l, r) => BatchAnd(exprToBatch(l), exprToBatch(r), a)
      case o @ Or(l, r) => BatchOr(exprToBatch(l), exprToBatch(r), o)
      case n @ Not(l) => BatchNot(exprToBatch(l), n)

      case _ =>
        throw new UnsupportedOperationException(s"unable to convert $expr to its batch version")
    }
  }
}
