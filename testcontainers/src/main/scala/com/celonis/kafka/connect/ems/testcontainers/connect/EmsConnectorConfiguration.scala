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
