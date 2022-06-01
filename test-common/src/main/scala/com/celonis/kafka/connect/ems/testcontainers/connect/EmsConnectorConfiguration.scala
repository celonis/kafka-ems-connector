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

package com.celonis.kafka.connect.ems.testcontainers.connect

import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.CONNECTOR_CLASS_KEY
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TASKS_MAX_KEY
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper

class EmsConnectorConfiguration(val name: String) {

  private val mapper: ObjectMapper = new ObjectMapper

  private val configNode = mapper.createObjectNode

  configNode.put(CONNECTOR_CLASS_KEY, "com.celonis.kafka.connect.ems.sink.EmsSinkConnector")
  configNode.put(TASKS_MAX_KEY, 1)

  def withConfig(key: String, value: Any): EmsConnectorConfiguration = {
    value match {
      case value: String  => configNode.put(key, value)
      case value: Long    => configNode.put(key, value)
      case value: Int     => configNode.put(key, value)
      case value: Double  => configNode.put(key, value)
      case value: Float   => configNode.put(key, value)
      case value: Boolean => configNode.put(key, value)
    }
    this
  }

  def toJson: String = {
    val conf: JsonNode = mapper.valueToTree[JsonNode](configNode)
    val connector = mapper.createObjectNode
      .put("name", name)
      .set("config", conf)
    mapper.writeValueAsString(connector)
  }
}

object EmsConnectorConfiguration {
  val TOPICS_KEY          = "topics"
  val CONNECTOR_CLASS_KEY = "connector.class"
  val TASKS_MAX_KEY       = "tasks.max"
}
