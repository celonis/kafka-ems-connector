/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.ParquetLocalInputFile
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnectionCut
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withParquetUploadLatency
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.mockserver.withMockResponse
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.parquet.hadoop.ParquetFileReader
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.output.OutputFrame.OutputType
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.WaitingConsumer

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ErrorPolicyTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

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
          (frame: OutputFrame) => frame.getUtf8String.contains("Error policy is set to RETRY."),
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

  test("honours connect.ems.sink.put.timeout.ms") {

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
      .withConfig(SINK_PUT_TIMEOUT_KEY, "250")

    withConnector(emsConnector) {
      withParquetUploadLatency(1.second) { // trigger SINK_PUT_TIMEOUT by injecting a 1 second latency
        sendDummyAvroRecord(sourceTopic)

        val consumer = new WaitingConsumer
        kafkaConnectContainer.followOutput(consumer, OutputType.STDOUT)
        consumer.waitUntil(
          (frame: OutputFrame) => frame.getUtf8String.contains("timed out after 250"),
          30,
          TimeUnit.SECONDS,
        )
      }

      eventually(timeout(60 seconds)) {
        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("FAILED")
      }
    }
  }

  test("continue on invalid input") {

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(COMMIT_RECORDS_KEY, 2)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "THROW")
      .withConfig(ERROR_CONTINUE_ON_INVALID_INPUT_KEY, true)
      .withConfig("value.converter.schemas.enable", "false")
      .withConfig("value.converter", "org.apache.kafka.connect.json.JsonConverter")
      .withConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")

    println(proxyServerUrl)

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {
      withConnector(emsConnector) {

        // The first error should not prevent other records to be ingested, even if they are part of the same put batch
        withStringStringProducer { producer =>
          producer.send(new ProducerRecord(sourceTopic, """{"":"missingKey"}"""))
          producer.send(new ProducerRecord(sourceTopic, """{"x":"validKey"}"""))
          producer.send(new ProducerRecord(sourceTopic, """{"x":"validKey"}"""))
        }

        eventually(timeout(20 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val fileReader   = ParquetFileReader.open(new ParquetLocalInputFile(parquetFile))

        fileReader.getRecordCount shouldBe 2
      }
    }
  }
}
