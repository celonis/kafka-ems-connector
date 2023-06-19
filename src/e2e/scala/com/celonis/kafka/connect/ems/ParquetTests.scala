/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.ParquetLocalInputFile
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.mockserver.withMockResponse
import io.confluent.kafka.serializers.AvroData
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.parquet.hadoop.ParquetFileReader
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.temporal.ChronoField
import java.time.temporal.TemporalField
import java.util
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ParquetTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  test("reads records from generated parquet file") {
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
      }
    }
  }

  test("works AVRO logical types in the input avro topic") {
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
        .withConfig(FLATTENER_ENABLE_KEY, true)

      withConnector(emsConnector) {

        // AVRO types
        val decimalSchema = SchemaBuilder.builder().bytesType()
        LogicalTypes.decimal(9, 5).addToSchema(decimalSchema)

        val dateSchema = SchemaBuilder.builder().intType()
        LogicalTypes.date().addToSchema(dateSchema)

        val timeMillisSchema = SchemaBuilder.builder().intType()
        LogicalTypes.timeMillis().addToSchema(timeMillisSchema)

        val timeMicrosSchema = SchemaBuilder.builder().longType()
        LogicalTypes.timeMicros().addToSchema(timeMicrosSchema)

        val timestampMillisSchema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema)

        val timestampMicrosSchema = SchemaBuilder.builder().longType()
        LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema)

        val uuidSchema = SchemaBuilder.builder().stringType()
        LogicalTypes.uuid().addToSchema(uuidSchema)

        // Field names
        val decimalField         = "decimal"
        val dateField            = "date"
        val timeMillisField      = "timeMillis"
        val timestampMillisField = "timestampMillis"
        val timeMicrosField      = "timeMicros"
        val timestampMicrosField = "timestampMicros"
        val uuidField            = "uuid"

        // AVRO schema
        val valueSchema = SchemaBuilder.record("record").fields()
          .name(decimalField).`type`(decimalSchema).noDefault()
          .name(dateField).`type`(dateSchema).noDefault()
          .name(timeMillisField).`type`(timeMillisSchema).noDefault()
          .name(timestampMillisField).`type`(timestampMillisSchema).noDefault()
          .name(timeMicrosField).`type`(timeMicrosSchema).noDefault()
          .name(timestampMicrosField).`type`(timestampMicrosSchema).noDefault()
          .name(uuidField).`type`(uuidSchema).noDefault()
          .endRecord()

        // Activate logical type conversions to be able to use higher-level types below
        AvroData.addLogicalTypeConversion(GenericData.get())

        // Build AVRO record
        val bigDecimal      = new java.math.BigDecimal(java.math.BigInteger.valueOf(123456789), 5)
        val date            = LocalDate.now()
        val timeMillis      = LocalTime.ofNanoOfDay(1_000_000 * 123)               // we just want millis precision
        val timestampMillis = Instant.ofEpochMilli(1686990713123L)                 // we just want millis precision
        val timeMicros      = LocalTime.ofNanoOfDay(1_000_000 * 123 + 1000)        // Micros precision
        val timestampMicros = Instant.ofEpochMilli(1686990713123L).plusNanos(1000) // Micros precision
        val uuid            = util.UUID.randomUUID()

        val writeRecord = new GenericData.Record(valueSchema)
        writeRecord.put(decimalField, bigDecimal)
        writeRecord.put(dateField, date)
        writeRecord.put(timeMillisField, timeMillis)
        writeRecord.put(timestampMillisField, timestampMillis)
        writeRecord.put(timeMicrosField, timeMicros)
        writeRecord.put(timestampMicrosField, timestampMicros)
        writeRecord.put(uuidField, uuid)

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, writeRecord)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val fileReader   = ParquetFileReader.open(new ParquetLocalInputFile(parquetFile))
        val schema       = fileReader.getFooter.getFileMetaData.getSchema
        fileReader.close()

        val reader = parquetReader(parquetFile)
        val record = reader.read()

        val decimalType = schema.getType(0)
        decimalType.toString shouldBe "optional binary decimal (DECIMAL(9,5))"
        record.get(decimalField) shouldBe bigDecimal

        val dateType = schema.getType(1)
        dateType.toString shouldBe "optional int32 date (DATE)"
        record.get(dateField) shouldBe date

        val timeMillisType = schema.getType(2)
        timeMillisType.toString shouldBe "optional int32 timeMillis (TIME(MILLIS,true))"
        record.get(timeMillisField) shouldBe timeMillis

        val timestampMillisType = schema.getType(3)
        timestampMillisType.toString shouldBe "optional int64 timestampMillis (TIMESTAMP(MILLIS,true))"
        record.get(timestampMillisField) shouldBe timestampMillis

        // ConnectData <-> Avro conversion does not support the following logical types, and only the underlying type is used
        val timeMicrosType = schema.getType(4)
        timeMicrosType.toString shouldBe "optional int64 timeMicros"
        record.get(timeMicrosField) shouldBe timeMicros.get(ChronoField.MICRO_OF_SECOND)

        val timestampMicrosType = schema.getType(5)
        timestampMicrosType.toString shouldBe "optional int64 timestampMicros"
        record.get(timestampMicrosField) shouldBe timestampMicros.get(
          ChronoField.MICRO_OF_SECOND,
        ) + timestampMicros.getEpochSecond * 1_000_000

        val uuidType = schema.getType(6)
        uuidType.toString shouldBe "optional binary uuid (STRING)"
        record.get(uuidField).toString shouldBe uuid.toString
      }
    }
  }

}
