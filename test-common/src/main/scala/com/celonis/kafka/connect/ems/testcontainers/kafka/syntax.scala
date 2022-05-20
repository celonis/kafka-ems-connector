/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers

import org.testcontainers.containers.KafkaContainer

package object syntax {

  implicit class KafkaContainerOps(kafkaContainer: KafkaContainer) {
    val kafkaPort = 9092
    def getInternalBootstrapServers: String =
      s"PLAINTEXT://${kafkaContainer.getNetworkAliases.get(0)}:$kafkaPort"
  }
}
