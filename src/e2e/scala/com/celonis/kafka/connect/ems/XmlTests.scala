/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.mockserver.withMockResponse
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class XmlTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("reads xml records") {
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
        .withConfig(SHA512_SALT_KEY, "something")
        .withConfig(FLATTENER_ENABLE_KEY, true)
        .withConfig("value.converter", "com.celonis.kafka.connect.ems.converter.XmlConverter")

      withConnector(emsConnector) {
        val xml =
          """
            |<root>
            |  <nested>
            |    <a_bool>true</a_bool>
            |    <an_int>3</an_int>
            |    <many>first</many>
            |    <many>second</many>
            |    <mixed id="123">
            |      <inner>abc</inner>
            |    </mixed>
            |  </nested>
            |</root>
            |""".stripMargin

        withStringStringProducer(_.send(new ProducerRecord(sourceTopic, xml)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val reader       = parquetReader(parquetFile)
        val record       = reader.read()

        // note how everything is a string
        record.get("nested_a_bool").toString shouldBe "true"
        record.get("nested_an_int").toString shouldBe "3"

        // multiple instances of the same xml element are grouped in a sequence
        record.get("nested_many").toString shouldBe """["first","second"]"""
        record.get("nested_mixed_id").toString shouldBe "123"
        record.get("nested_mixed_inner").toString shouldBe "abc"
      }
    }
  }

}
