/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.extractParquetFrom
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectEnvironment
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ParquetTests extends AnyFunSuite with KafkaConnectEnvironment with Matchers {

  test("read generated parquet file") {

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

        val randomString = UUID.randomUUID().toString
        val randomInt    = scala.util.Random.nextInt()

        val valueSchema = SchemaBuilder.record("record").fields()
          .requiredString("field1")
          .requiredInt("field2")
          .endRecord()

        val writeRecord = new GenericData.Record(valueSchema)
        writeRecord.put("field1", randomString)
        writeRecord.put("field2", randomInt)

        stringAvroProducer.send(new ProducerRecord(sourceTopic, writeRecord))
        stringAvroProducer.flush()

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFrom(httpRequests.head)
        val reader       = parquetReader(parquetFile)
        val record       = reader.read()

        record.get("field1").toString should be(randomString)
        record.get("field2").asInstanceOf[Int] should be(randomInt)
      }
    }
  }

}
