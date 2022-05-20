package com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures

import com.celonis.kafka.connect.ems.testcontainers.connect.{EmsConnectorConfiguration, KafkaConnectClient}
import org.testcontainers.containers.ToxiproxyContainer

object connect {

  def withConnectionCut(testCode: => Any)(implicit proxy: ToxiproxyContainer.ContainerProxy): Unit = {
    proxy.setConnectionCut(true)
    try {
      val _ = testCode
    } finally {
      proxy.setConnectionCut(false)
    }
  }

  def withConnector(
    connectorConfig: EmsConnectorConfiguration,
  )(testCode:        => Any,
  )(
    implicit
    kafkaConnectClient: KafkaConnectClient,
  ): Unit = {
    kafkaConnectClient.registerConnector(connectorConfig)
    try {
      kafkaConnectClient.waitConnectorInRunningState(connectorConfig.name)
      val _ = testCode
    } finally {
      kafkaConnectClient.deleteConnector(connectorConfig.name)
    }
  }
}
