/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectEnvironment
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.output.OutputFrame.OutputType

import java.util.UUID
import scala.concurrent.duration.DurationInt
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.WaitingConsumer

import java.util.concurrent.TimeUnit

class SmokeTests extends AnyFunSuite with KafkaConnectEnvironment with Matchers {

  test("one record upload") {

    val sourceTopic = "topic-" + UUID.randomUUID()

    val emsRequest = HttpRequest.request()
      .withPath("/")
      .withQueryStringParameter("targetName", "ems-table")

    // mock ems response
    val mockResponse = HttpResponse.response().withBody(
      """
        |{
        |  "id" : "7513bc96-7003-480e-9b72-4e4ae8a7b05c",
        |  "fileName" : "my-parquet-nice-file-name.parquet",
        |  "bucketId" : "6ca836f9-12e3-46f0-a5c4-20c9a309833d",
        |  "flushStatus" : "NEW",
        |  "clientId" : null,
        |  "fallbackVarcharLength" : 10000,
        |  "upsertStrategy" : null
        |}
        |""".stripMargin,
    )

    withMockResponse(emsRequest, mockResponse) {

      val emsConnector = new EmsConnectorConfiguration("ems")
        .withConfig(TOPICS_KEY, sourceTopic)
        .withConfig(ENDPOINT_KEY, proxyServerUrl)
        .withConfig(AUTHORIZATION_KEY, "AppKey key")
        .withConfig(TARGET_TABLE_KEY, "ems-table")
        .withConfig(COMMIT_RECORDS_KEY, 1)
        .withConfig(COMMIT_SIZE_KEY, 1000000L)
        .withConfig(COMMIT_INTERVAL_KEY, 3600000)
        .withConfig(TMP_DIRECTORY_KEY, "/tmp/")

      withConnector(emsConnector) {
        // produce a record to the source topic
        sendDummyAvroRecord(sourceTopic)

        // assert
        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequest, VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }
      }
    }
  }

  test("throw error policy") {

    val sourceTopic = "topic-" + UUID.randomUUID()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, "ems-table")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "THROW")

    withConnector(emsConnector) {
      // produce a record to the source topic
      sendDummyAvroRecord(sourceTopic)

      // assert
      val emsRequest = HttpRequest.request()
        .withPath("/")
        .withQueryStringParameter("targetName", "ems-table")

      eventually(timeout(60 seconds), interval(1 seconds)) {
        mockServerClient.verify(emsRequest, VerificationTimes.once())
        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("FAILED")
      }
    }
  }

  test("continue error policy") {

    val sourceTopic = "topic-" + UUID.randomUUID()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, "ems-table")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "CONTINUE")

    withConnector(emsConnector) {
      // produce a record to the source topic
      sendDummyAvroRecord(sourceTopic)

      // assert
      val emsRequest = HttpRequest.request()
        .withPath("/")
        .withQueryStringParameter("targetName", "ems-table")

      eventually(timeout(60 seconds), interval(1 seconds)) {
        mockServerClient.verify(emsRequest, VerificationTimes.once())
        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("RUNNING")
      }
    }
  }

  test("retry error policy") {

    val sourceTopic = "topic-" + UUID.randomUUID()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, "ems-table")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "RETRY")

    withConnector(emsConnector) {

      withConnectionCut {
        // produce a record to the source topic
        sendDummyAvroRecord(sourceTopic)

        val consumer = new WaitingConsumer
        kafkaConnectContainer.followOutput(consumer, OutputType.STDOUT)
        consumer.waitUntil(
          (frame: OutputFrame) => frame.getUtf8String.contains("Error policy set to RETRY."),
          30,
          TimeUnit.SECONDS,
        )
      }

      // assert
      val emsRequest = HttpRequest.request()
        .withMethod("POST")
        .withPath("/")
        .withQueryStringParameter("targetName", "ems-table")

      // mock ems response
      val mockResponse = HttpResponse.response().withBody(
        """
          |{
          |  "id" : "7513bc96-7003-480e-9b72-4e4ae8a7b05c",
          |  "fileName" : "my-parquet-nice-file-name.parquet",
          |  "bucketId" : "6ca836f9-12e3-46f0-a5c4-20c9a309833d",
          |  "flushStatus" : "NEW",
          |  "clientId" : null,
          |  "fallbackVarcharLength" : 10000,
          |  "upsertStrategy" : null
          |}
          |""".stripMargin,
      )

      withMockResponse(emsRequest, mockResponse) {
        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequest, VerificationTimes.atMost(2))
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }
      }
    }
  }
}
