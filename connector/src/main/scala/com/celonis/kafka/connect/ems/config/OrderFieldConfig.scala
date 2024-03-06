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

package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadataFieldInserter

case class OrderFieldConfig(name: Option[String])

object OrderFieldConfig {
  def from(props: Map[String, _], primaryKeys: List[String]): OrderFieldConfig = {
    val configFieldName = PropertiesHelper.getString(props, ORDER_FIELD_NAME_KEY).map(_.trim).filter(_.nonEmpty)
    val name =
      if (primaryKeys.nonEmpty)
        configFieldName.orElse(
          Some(EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName),
        )
      else None

    OrderFieldConfig(name)
  }
}
