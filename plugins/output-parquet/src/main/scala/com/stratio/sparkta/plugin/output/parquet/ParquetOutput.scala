/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparkta.plugin.output.parquet

import java.io.{Serializable => JSerializable}
import scala.util.Try

import com.github.nscala_time.time.StaticDateTime
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.{Logging, SparkContext}

import com.stratio.sparkta.sdk.TypeOp._
import com.stratio.sparkta.sdk.ValidatingPropertyMap._
import com.stratio.sparkta.sdk.WriteOp.WriteOp
import com.stratio.sparkta.sdk._

/**
 * This output save as parquet file the information.
 * @param keyName
 * @param properties
 * @param sparkContext
 * @param operationTypes
 * @param bcSchema
 */
class ParquetOutput(keyName: String,
                    properties: Map[String, JSerializable],
                    @transient sparkContext: SparkContext,
                    operationTypes: Option[Broadcast[Map[String, (WriteOp, TypeOp)]]],
                    bcSchema: Option[Broadcast[Seq[TableSchema]]],
                    timeName: String)
  extends Output(keyName, properties, sparkContext, operationTypes, bcSchema, timeName: String) with Logging {

  override val supportedWriteOps = Seq(WriteOp.Inc, WriteOp.IncBig, WriteOp.Set, WriteOp.Max, WriteOp.Min,
    WriteOp.AccAvg, WriteOp.AccMedian, WriteOp.AccVariance, WriteOp.AccStddev, WriteOp.FullText, WriteOp.AccSet)

  override val multiplexer = Try(properties.getString("multiplexer").toBoolean).getOrElse(false)

  override val isAutoCalculateId = Try(properties.getString("isAutoCalculateId").toBoolean).getOrElse(false)

  final val Slash: String = "/"

  def timeSuffix: String = Slash + DateOperations.dateFromGranularity(StaticDateTime.now, timeName)

  val path = properties.getString("path", None)

  override def upsert(dataFrame: DataFrame, tableName: String): Unit = {
    require(path.isDefined, bcSchema.isDefined)
    dataFrame.save(s"${path.get}$timeSuffix", "parquet", SaveMode.Append)
  }
}
