/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.types.StructType

trait RewriteRowLevelIcebergCommand extends RewriteRowLevelCommand {

  override protected def buildWriteDeltaProjections(
      plan: LogicalPlan,
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): WriteDeltaProjections = {

    val rowProjection = if (rowAttrs.nonEmpty) {
      Some(lazyProjection(plan, rowAttrs))
    } else {
      None
    }

    val rowIdProjection = lazyProjection(plan, rowIdAttrs)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      Some(lazyProjection(plan, metadataAttrs))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  // the projection is done by name, ignoring expr IDs
  private def lazyProjection(
      plan: LogicalPlan,
      projectedAttrs: Seq[Attribute]): ProjectingInternalRow = {

    val projectedOrdinals = projectedAttrs.map(attr => plan.output.indexWhere(_.name == attr.name))
    val schema = StructType.fromAttributes(projectedOrdinals.map(plan.output(_)))
    ProjectingInternalRow(schema, projectedOrdinals)
  }
}
