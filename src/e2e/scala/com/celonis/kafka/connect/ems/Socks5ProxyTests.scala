/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.testcontainers.Socks5ProxyContainer
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.ems.withMockResponse
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class Socks5ProxyTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("socks5 proxy support") {

    val s5c = new Socks5ProxyContainer(network)
    s5c.start()

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {

      val emsConnector = new EmsConnectorConfiguration("ems")
        .withConfig(TOPICS_KEY, sourceTopic)
        .withConfig(ENDPOINT_KEY, proxyServerUrl)
        .withConfig(AUTHORIZATION_KEY, "AppKey key")
        .withConfig(TARGET_TABLE_KEY, emsTable)
        .withConfig(COMMIT_RECORDS_KEY, 1)
        .withConfig(COMMIT_SIZE_KEY, 1000000L)
        .withConfig(COMMIT_INTERVAL_KEY, 3600000)
        .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
        .withConfig(PROXY_HOST_KEY, Socks5ProxyContainer.networkAlias)
        .withConfig(PROXY_PORT_KEY, Socks5ProxyContainer.port)
        .withConfig(PROXY_TYPE_KEY, "SOCKS5")

      withConnector(emsConnector) {
        sendDummyAvroRecord(sourceTopic)

        // assert
        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }
      }
    }
    s5c.stop()
  }

  test("socks5 proxy with authentication") {

    val proxyUser = "user"
    val proxyPass = "pass"
    val s5c       = new Socks5ProxyContainer(network, proxyUser, proxyPass)
    s5c.start()

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {

      val emsConnector = new EmsConnectorConfiguration("ems")
        .withConfig(TOPICS_KEY, sourceTopic)
        .withConfig(ENDPOINT_KEY, proxyServerUrl)
        .withConfig(AUTHORIZATION_KEY, "AppKey key")
        .withConfig(TARGET_TABLE_KEY, emsTable)
        .withConfig(COMMIT_RECORDS_KEY, 1)
        .withConfig(COMMIT_SIZE_KEY, 1000000L)
        .withConfig(COMMIT_INTERVAL_KEY, 3600000)
        .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
        .withConfig(PROXY_HOST_KEY, Socks5ProxyContainer.networkAlias)
        .withConfig(PROXY_PORT_KEY, Socks5ProxyContainer.port)
        .withConfig(PROXY_TYPE_KEY, "SOCKS5")
        .withConfig(PROXY_AUTHENTICATION_KEY, "BASIC")
        .withConfig(PROXY_AUTHBASIC_USERNAME_KEY, proxyUser)
        .withConfig(PROXY_AUTHBASIC_PASSWORD_KEY, proxyPass)

      withConnector(emsConnector) {

        sendDummyAvroRecord(sourceTopic)

        // assert
        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }
      }
      s5c.stop()
    }
  }
}
