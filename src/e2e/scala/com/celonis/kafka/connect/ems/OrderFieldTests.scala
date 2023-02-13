/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.mockserver.withMockResponse
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

class OrderFieldTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("The order field is not inserted when PK was not set") {
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
        .withConfig(OBFUSCATED_FIELDS_KEY, "field1")
        .withConfig(OBFUSCATION_TYPE_KEY, "shA512")

      withConnector(emsConnector) {
        val randomInt = scala.util.Random.nextInt()

        val valueSchema = SchemaBuilder.record("record").fields()
          .requiredString("field1")
          .requiredInt("field2")
          .endRecord()

        val writeRecord = new GenericData.Record(valueSchema)
        writeRecord.put("field1", "myfieldvalue")
        writeRecord.put("field2", randomInt)

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, writeRecord)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val reader       = parquetReader(parquetFile)
        val record       = reader.read()

        record.get("field1").toString should be(
          "ade2426de954b6cd28ce00c83b931c1943ce87fbc421897156c4be6c07e1b83e6618a842c406ba7c0bf806fee3ae3164c8aac873ff1ac113a6ceb66e0bb12224",
        )
        record.get("field2").asInstanceOf[Int] should be(randomInt)
        record.hasField(CelonisOrderFieldName) should be(false)
      }
    }
  }

  test("Introduce the auto generate sortable field when a PKs list was provided") {
    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()

    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {

      val emsConnector = new EmsConnectorConfiguration("ems")
        .withConfig(TOPICS_KEY, sourceTopic)
        .withConfig(ENDPOINT_KEY, proxyServerUrl)
        .withConfig(AUTHORIZATION_KEY, "AppKey key")
        .withConfig(TARGET_TABLE_KEY, emsTable)
        .withConfig(COMMIT_RECORDS_KEY, 3)
        .withConfig(COMMIT_SIZE_KEY, 1000000L)
        .withConfig(COMMIT_INTERVAL_KEY, 3600000)
        .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
        .withConfig(PRIMARY_KEYS_KEY, "field1")

      withConnector(emsConnector) {
        val randomInt = scala.util.Random.nextInt()

        val valueSchema = SchemaBuilder.record("record").fields()
          .requiredString("field1")
          .requiredInt("field2")
          .endRecord()

        val writeRecord1 = new GenericData.Record(valueSchema)
        writeRecord1.put("field1", "myvalue1")
        writeRecord1.put("field2", 1)

        val writeRecord2 = new GenericData.Record(valueSchema)
        writeRecord2.put("field1", "myvalue2")
        writeRecord2.put("field2", 2)

        val writeRecord3 = new GenericData.Record(valueSchema)
        writeRecord3.put("field1", "myvalue1")
        writeRecord3.put("field2", 3)

        withStringAvroProducer { p =>
          p.send(new ProducerRecord(sourceTopic, writeRecord1))
          p.send(new ProducerRecord(sourceTopic, writeRecord2))
          p.send(new ProducerRecord(sourceTopic, writeRecord3))
        }

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val reader       = parquetReader(parquetFile)

        val record1 = reader.read()
        record1.getSchema.getFields.asScala.map(_.name()).toSet shouldBe Set("field1", "field2", CelonisOrderFieldName)
        record1.get("field1").toString shouldBe "myvalue1"
        record1.get("field2").asInstanceOf[Int] shouldBe 1
        record1.get(CelonisOrderFieldName).asInstanceOf[Long] shouldBe 0

        val record2 = reader.read()
        record2.getSchema.getFields.asScala.map(_.name()).toSet shouldBe Set("field1", "field2", CelonisOrderFieldName)
        record2.get("field1").toString shouldBe "myvalue2"
        record2.get("field2").asInstanceOf[Int] shouldBe 2
        record2.get(CelonisOrderFieldName).asInstanceOf[Long] shouldBe 1

        val record3 = reader.read()
        record3.getSchema.getFields.asScala.map(_.name()).toSet shouldBe Set("field1", "field2", CelonisOrderFieldName)
        record3.get("field1").toString shouldBe "myvalue1"
        record3.get("field2").asInstanceOf[Int] shouldBe 3
        record3.get(CelonisOrderFieldName).asInstanceOf[Long] shouldBe 2

      }
    }
  }

  test("Uses the provided PK field when PK was set") {
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
        .withConfig(PRIMARY_KEYS_KEY, "field1")
        .withConfig(ORDER_FIELD_NAME_KEY, "field1")

      withConnector(emsConnector) {
        val randomInt = scala.util.Random.nextInt()

        val valueSchema = SchemaBuilder.record("record").fields()
          .requiredString("field1")
          .requiredInt("field2")
          .endRecord()

        val writeRecord = new GenericData.Record(valueSchema)
        writeRecord.put("field1", "myfieldvalue")
        writeRecord.put("field2", 1)

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, writeRecord)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val reader       = parquetReader(parquetFile)
        val record       = reader.read()

        record.get("field1").toString shouldBe "myfieldvalue"
        record.get("field2").asInstanceOf[Int] shouldBe 1
        record.getSchema.getFields.asScala.map(_.name()).toSet shouldBe Set("field1", "field2")
      }
    }
  }

}
