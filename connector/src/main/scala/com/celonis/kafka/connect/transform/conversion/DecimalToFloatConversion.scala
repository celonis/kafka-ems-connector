/*
 * Copyright 2023 Celonis SE
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
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

/** Convert Bytes with Logical Type Decimal to Doubles
  */
object DecimalToFloatConversion extends ConnectConversion {
  override def convertSchema(originalSchema: Schema): Schema = (originalSchema.`type`(), originalSchema.name()) match {
    case (Schema.Type.BYTES, Decimal.LOGICAL_NAME) =>
      val newSchema = SchemaBuilder.float64()
      if (originalSchema.isOptional) newSchema.optional()
      newSchema.build()
    case _ => originalSchema
  }

  override def convertValue(connectValue: Any, originalSchema: Schema, targetSchema: Schema): Any =
    (connectValue, originalSchema.`type`(), targetSchema.`type`()) match {
      case (decimal: java.math.BigDecimal, Schema.Type.BYTES, Schema.Type.FLOAT64) => decimal.doubleValue()
      case _                                                                       => connectValue
    }
}
