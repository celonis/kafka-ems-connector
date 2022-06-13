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
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.model.HttpRequest
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class SchemaCompatibilityTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("handle schema column drop") {

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

      withConnector(emsConnector) {

        val schema_v1 = SchemaBuilder.record("record").fields()
          .requiredString("name")
          .requiredLong("count")
          .requiredBoolean("flag")
          .endRecord()

        val schema_v2 = SchemaBuilder.record("record").fields()
          .requiredLong("count")
          .requiredBoolean("flag")
          .endRecord()

        val record_v1 = new GenericData.Record(schema_v1)
        record_v1.put("name", "string")
        record_v1.put("count", 999)
        record_v1.put("flag", false)

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, record_v1)))

        val record_v2 = new GenericData.Record(schema_v2)
        record_v2.put("count", 1000)
        record_v2.put("flag", false)

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, record_v2)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.atLeast(2))
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))

        val record = extractAvroRecordFromRequest(httpRequests(0))
        record.get("name").toString should be("string")
        record.get("count").asInstanceOf[Long] should be(999)

        val record1 = extractAvroRecordFromRequest(httpRequests(1))
        record1.get("count").asInstanceOf[Long] should be(1000)
      }
    }
  }

  def extractAvroRecordFromRequest(req: HttpRequest): GenericRecord = {
    val parquetFile = extractParquetFromRequest(req)
    val reader      = parquetReader(parquetFile)
    reader.read()
  }
}
