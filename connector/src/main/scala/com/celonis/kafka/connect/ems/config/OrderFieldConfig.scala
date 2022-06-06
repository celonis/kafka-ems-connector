/*
 * Copyright 2022 Celonis SE
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

import cats.syntax.option._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.ems.conversion.NoOpOrderFieldInserter
import com.celonis.kafka.connect.ems.conversion.OrderFieldInserter

case class OrderFieldConfig(name: Option[String], inserter: OrderFieldInserter)

object OrderFieldConfig {
  def from(props: Map[String, _], primaryKeys: List[String]): OrderFieldConfig = {
    val configFieldName = PropertiesHelper.getString(props, ORDER_FIELD_NAME_KEY).map(_.trim).filter(_.nonEmpty)
    val name =
      if (primaryKeys.nonEmpty)
        configFieldName.orElse(
          OrderFieldInserter.FieldName.some,
        )
      else None

    val inserter = if (primaryKeys.nonEmpty) {
      // if a value is set by the user, then we don't use the auto-injected one. It is expected the field name is
      // present since the connector won't validate it
      configFieldName.map(_ => NoOpOrderFieldInserter).getOrElse(OrderFieldInserter)
    } else {
      // we don't have a PK, therefore do not use any inserter
      NoOpOrderFieldInserter
    }
    OrderFieldConfig(name, inserter)
  }
}
