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

class SchemaChangeTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("schema change just after a flush") {
    val sourceTopic  = randomTopicName()
    val emsTable     = randomEmsTable()
    val emsConnector = buildConnector(sourceTopic, emsTable)

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {

      withConnector(emsConnector) {

        // After the insertion of the first two records, a flush happens because
        // ems.connect.commit.records is set to 2
        withStringStringProducer {
          producer =>
            producer.send(new ProducerRecord(sourceTopic, xml1))
            producer.send(new ProducerRecord(sourceTopic, xml1))
        }

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
        }

        withStringStringProducer {
          producer =>
            producer.send(new ProducerRecord(sourceTopic, xml2))
            producer.send(new ProducerRecord(sourceTopic, xml2))
        }

        eventually(timeout(10 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.exactly(2))
        }
      }
    }
  }

  test("schema change when previous file has not been flushed yet") {
    val sourceTopic  = randomTopicName()
    val emsTable     = randomEmsTable()
    val emsConnector = buildConnector(sourceTopic, emsTable)

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {

      withConnector(emsConnector) {

        withStringStringProducer {
          producer =>
            producer.send(new ProducerRecord(sourceTopic, xml1))
            producer.send(new ProducerRecord(sourceTopic, xml2))
        }

        eventually(timeout(10 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
        }

        withStringStringProducer {
          producer =>
            producer.send(new ProducerRecord(sourceTopic, xml2))
            producer.send(new ProducerRecord(sourceTopic, xml2))
            producer.send(new ProducerRecord(sourceTopic, xml2))
        }

        eventually(timeout(10 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.exactly(3))
        }
      }
    }
  }

  private def buildConnector(sourceTopic: String, emsTable: String): EmsConnectorConfiguration =
    new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(COMMIT_RECORDS_KEY, 2)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(SHA512_SALT_KEY, "something")
      .withConfig(FLATTENER_ENABLE_KEY, true)
      .withConfig("value.converter", "com.celonis.kafka.connect.ems.converter.XmlConverter")

  lazy val xml1: String =
    """
      |<root>
      |  <a>1</a>
      |</root>
      |""".stripMargin

  lazy val xml2: String =
    """
      |<root>
      |  <a>2</a>
      |  <b>3</b>
      |</root>
      |""".stripMargin

}
