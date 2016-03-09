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

package org.apache.spark.sql.execution.vector

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.execution.{PhysicalRDD, LeafNode}
import org.apache.spark.sql.sources.{HadoopFsRelation, BaseRelation}

/** Physical plan node for scanning data from an RDD. */
private[sql] case class PhysicalBatchRDD(
    output: Seq[Attribute],
    rdd: RDD[RowBatch],
    override val nodeName: String,
    override val metadata: Map[String, String] = Map.empty)
  extends LeafNode {

  override def outputsRowBatches: Boolean = true
  override def outputsUnsafeRows: Boolean = false
  override def canProcessRowBatches: Boolean = true
  override def canProcessSafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = false

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(getClass.getName)
//    doBatchExecute().mapPartitions { iter =>
//      iter.map(_.rowIterator().asScala).flatten
//    }

  protected override def doBatchExecute(): RDD[RowBatch] = rdd

  override def simpleString: String = {
    val metadataEntries = for ((key, value) <- metadata.toSeq.sorted) yield s"$key: $value"
    s"BatchScan $nodeName${output.mkString("[", ",", "]")}" +
      s"${metadataEntries.mkString(" ", ", ", "")}"
  }
}

private[sql] object PhysicalBatchRDD {
  // Metadata keys
  val INPUT_PATHS = "InputPaths"
  val PUSHED_FILTERS = "PushedFilters"

  def createFromDataSource(
    output: Seq[Attribute],
    rdd: RDD[RowBatch],
    relation: BaseRelation,
    metadata: Map[String, String] = Map.empty): PhysicalBatchRDD = {
    PhysicalBatchRDD(output, rdd, relation.toString, metadata)
  }
}
