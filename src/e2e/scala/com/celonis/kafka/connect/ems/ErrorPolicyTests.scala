/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectEnvironment
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.output.OutputFrame.OutputType
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.WaitingConsumer

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ErrorPolicyTests extends AnyFunSuite with KafkaConnectEnvironment with Matchers {

  test("throw error policy") {

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "THROW")

    withConnector(emsConnector) {

      sendDummyAvroRecord(sourceTopic)

      eventually(timeout(60 seconds), interval(1 seconds)) {
        mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("FAILED")
      }
    }
  }

  test("continue error policy") {

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "CONTINUE")

    withConnector(emsConnector) {

      sendDummyAvroRecord(sourceTopic)

      eventually(timeout(60 seconds), interval(1 seconds)) {
        mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("RUNNING")
      }
    }
  }

  test("retry error policy") {

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "RETRY")

    withConnector(emsConnector) {

      withConnectionCut {

        sendDummyAvroRecord(sourceTopic)

        val consumer = new WaitingConsumer
        kafkaConnectContainer.followOutput(consumer, OutputType.STDOUT)
        consumer.waitUntil(
          (frame: OutputFrame) => frame.getUtf8String.contains("Error policy set to RETRY."),
          30,
          TimeUnit.SECONDS,
        )
      }

      val emsRequest = emsRequestForTable(emsTable)

      withMockResponse(emsRequest, mockEmsResponse) {
        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequest, VerificationTimes.atMost(2))
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }
      }
    }
  }
}