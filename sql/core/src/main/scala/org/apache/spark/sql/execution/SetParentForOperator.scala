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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.rules.Rule

private[sql] object SetParentForOperator extends Rule[SparkPlan]  {

  override def apply(plan: SparkPlan): SparkPlan = {
    var i: Int = 0
    plan.transformDown {
      case bi: BinaryNode =>
        bi.left.setParent(bi)
        bi.right.setParent(bi)
        bi.setId(i)
        i += 1
        bi
      case ui: UnaryNode =>
        ui.child.setParent(ui)
        ui.setId(i)
        i += 1
        ui
      case o =>
        o.setId(i)
        i += 1
        o
    }
  }
}
