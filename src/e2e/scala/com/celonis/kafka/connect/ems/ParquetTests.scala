/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.ParquetLocalInputFile
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.storage.ValueAndSchemas
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.mockserver.withMockResponse
import io.confluent.kafka.serializers.AvroData
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.connect.data
import org.apache.parquet.hadoop.ParquetFileReader
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ParquetTests extends AnyFunSuite with KafkaConnectContainerPerSuite with SampleData with Matchers {
  test("generate parquet file with obfuscated fields") {
    val emsConnector =
      new EmsConnectorConfiguration("ems")
        .withConfig(ENDPOINT_KEY, proxyServerUrl)
        .withConfig(AUTHORIZATION_KEY, "AppKey key")
        .withConfig(COMMIT_RECORDS_KEY, 1)
        .withConfig(COMMIT_SIZE_KEY, 1000000L)
        .withConfig(COMMIT_INTERVAL_KEY, 3600000)
        .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
        .withConfig(SHA512_SALT_KEY, "something")
        .withConfig(OBFUSCATED_FIELDS_KEY, "field1")
        .withConfig(OBFUSCATION_TYPE_KEY, "shA512")
        // This has been renamed and should be ignored and not make the connector registration fail
        .withConfig("connect.ems.parquet.write.flush.records", 1)

    val randomInt = scala.util.Random.nextInt()
    val expectations = List(
      ValueAndSchemas(
        name         = "field1",
        avroValue    = "myfieldvalue",
        connectValue = "myfieldvalue",
        // parquet value is obfuscated
        parquetValue = new Utf8(
          "ade2426de954b6cd28ce00c83b931c1943ce87fbc421897156c4be6c07e1b83e6618a842c406ba7c0bf806fee3ae3164c8aac873ff1ac113a6ceb66e0bb12224",
        ),
        avroSchema           = Schema.create(Schema.Type.STRING),
        connectSchemaBuilder = data.SchemaBuilder.string(),
        parquetSchema        = "required binary field1 (STRING)",
      ),
      ValueAndSchemas(
        name                 = "field2",
        avroValue            = randomInt,
        connectValue         = randomInt,
        parquetValue         = randomInt,
        avroSchema           = Schema.create(Schema.Type.INT),
        connectSchemaBuilder = data.SchemaBuilder.int32(),
        parquetSchema        = "required int32 field2",
      ),
    )

    testParquetValuesAndSchemas(emsConnector, expectations)
  }

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

    val emsConnectorConfig = new EmsConnectorConfiguration("ems")
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(FLATTENER_ENABLE_KEY, true)

    // We expect parquet types to be optional when flattening
    val expectedValues =
      primitiveValuesAndSchemas.map(value => value.copy(parquetSchema = "optional " + value.parquetSchema))

    testParquetValuesAndSchemas(emsConnectorConfig, expectedValues)
  }

  test("perform Decimal to Double conversion when flag is enabled") {
    // We expect parquet types to be required when not flattening
    val expectedValues = primitiveValuesAndSchemasWithDecimalConvertedToDouble.map(value =>
      value.copy(parquetSchema = "required " + value.parquetSchema),
    )

    val emsConnectorConfig = new EmsConnectorConfiguration("ems")
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(DECIMAL_CONVERSION_KEY, true) // Activate decimal conversion

    testParquetValuesAndSchemas(emsConnectorConfig, expectedValues)
  }

  /** For each ValueAndSchemas expectation, test that for the input AVRO schema/value, the output parquet schema/value
    * is what is expected
    */
  private def testParquetValuesAndSchemas(
    connectorConfig: EmsConnectorConfiguration,
    expectations:    List[ValueAndSchemas]): Unit = {
    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()
    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {
      connectorConfig
        .withConfig(TARGET_TABLE_KEY, emsTable)
        .withConfig(TOPICS_KEY, sourceTopic)

      withConnector(connectorConfig) {

        // AVRO schema
        val valueSchema = expectations.foldLeft(SchemaBuilder.record("record").fields()) {
          case (builder, valueAndSchema) =>
            builder.name(valueAndSchema.name).`type`(valueAndSchema.avroSchema).noDefault()
        }.endRecord()

        // Activate logical type conversions to be able to use higher-level types below
        AvroData.addLogicalTypeConversion(GenericData.get())

        // AVRO value
        val writeRecord = new GenericData.Record(valueSchema)
        expectations.foreach { valueAndSchema =>
          writeRecord.put(valueAndSchema.name, valueAndSchema.avroValue)
        }

        withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, writeRecord)))

        eventually(timeout(60 seconds), interval(1 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(connectorConfig.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        val fileReader   = ParquetFileReader.open(new ParquetLocalInputFile(parquetFile))
        val schema       = fileReader.getFooter.getFileMetaData.getSchema
        fileReader.close()

        val reader = parquetReader(parquetFile)
        val record = reader.read()

        expectations.zipWithIndex.foreach { case (valueAndSchema, index) =>
          withClue(valueAndSchema) {
            val parquetType = schema.getType(index)

            parquetType.toString shouldBe valueAndSchema.parquetSchema
            record.get(valueAndSchema.name) shouldBe valueAndSchema.parquetValue
          }

        }
      }
    }
  }
}
