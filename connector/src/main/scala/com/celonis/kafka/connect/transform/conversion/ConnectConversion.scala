/*
 * Copyright 2024 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.transform.conversion

import com.celonis.kafka.connect.transform.PreConversionConfig
import org.apache.kafka.connect.data.Schema

trait ConnectConversion {
  def convertSchema(originalSchema: Schema): Schema
  def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any
  final def convert(value: Any, originalSchema: Option[Schema]): (Any, Option[Schema]) =
    originalSchema match {
      case Some(originalSchema) =>
        val targetSchema = convertSchema(originalSchema)
        (convertValue(value, originalSchema, targetSchema), Some(targetSchema))
      case None => (value, None)
    }
}

object ConnectConversion {
  def fromConfig(config: PreConversionConfig): ConnectConversion =
    if (config.convertDecimalsToFloat) new RecursiveConversion(DecimalToFloatConversion)
    else noOpConversion

  val noOpConversion: ConnectConversion = new ConnectConversion {
    override def convertSchema(originalSchema: Schema): Schema = originalSchema
    override def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any = connectValue
  }
}
