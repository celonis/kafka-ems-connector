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

package com.celonis.kafka.connect.transform.flatten
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import org.apache.kafka.connect.data.Schema

/** Add inference and normalisation on top of an existing flattener
  */
private final class NormalisingFlattener(private[flatten] val innerFlattener: Flattener) extends Flattener {
  override def flatten(originalValue: Any, originalSchema: Option[Schema]): Any = {
    val valueAndSchema = originalSchema match {
      case Some(originalSchema) => ValueAndSchema(originalValue, originalSchema)
      case None =>
        InferSchemaAndNormaliseValue(originalValue).getOrElse(ValueAndSchema(originalValue, Schema.BYTES_SCHEMA))
    }

    innerFlattener.flatten(valueAndSchema.normalisedValue, Some(valueAndSchema.schema))
  }
}
