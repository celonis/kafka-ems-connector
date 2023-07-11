package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.parquet.extractParquetFromRequest
import com.celonis.kafka.connect.ems.storage.SampleData
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

import java.io.File
import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ParquetFileSizeTest extends AnyFunSuite with KafkaConnectContainerPerSuite with SampleData with Matchers {
  test("commit.size.bytes > uploaded file size bytes < (row.group.size.bytes + commit.size.bytes)") {
    val commitSize   = 100_000
    val rowGroupSize = 50_000
    val emsConnectorConfig = new EmsConnectorConfiguration("ems")
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(COMMIT_SIZE_KEY, commitSize)
      .withConfig(COMMIT_RECORDS_KEY, 50_000_000) //
      .withConfig(COMMIT_INTERVAL_KEY, 900_000)   // deliberately high, to force
      .withConfig(PARQUET_ROW_GROUP_SIZE_BYTES_KEY, rowGroupSize)
      .withConfig(DEBUG_KEEP_TMP_FILES_KEY, true)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(FLATTENER_ENABLE_KEY, true)

    testUploadedParquetFile(emsConnectorConfig, recordsToProduce = 1_000_000) {
      file =>
        assert(file.length() > commitSize)
        assert(file.length() < commitSize + rowGroupSize)
    }
  }

  private def testUploadedParquetFile(
    connectorConfig:  EmsConnectorConfiguration,
    recordsToProduce: Int,
  )(fileAssertion: File => Unit): Unit = {
    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()
    withMockResponse(emsRequestForTable(emsTable), mockEmsResponse) {
      connectorConfig
        .withConfig(TARGET_TABLE_KEY, emsTable)
        .withConfig(TOPICS_KEY, sourceTopic)

      withConnector(connectorConfig) {

        // AVRO schema
        val valueSchema = SchemaBuilder.record("record").fields()
          .optionalInt("an_int")
          .optionalString("a_string")
          .optionalLong("a_long")
          .endRecord()

        // AVRO values
        withStringAvroProducer { producer =>
          LazyList.continually(UUID.randomUUID()).take(recordsToProduce).foreach { id =>
            val writeRecord = new GenericData.Record(valueSchema)
            writeRecord.put("an_int", util.Random.nextInt())
            writeRecord.put("a_string", s"record $id")
            writeRecord.put("a_long", util.Random.nextLong())
            producer.send(new ProducerRecord(sourceTopic, writeRecord))
          }
        }

        eventually(timeout(3.minutes), interval(25 seconds)) {
          mockServerClient.verify(emsRequestForTable(emsTable), VerificationTimes.once())
          val status = kafkaConnectClient.getConnectorStatus(connectorConfig.name)
          status.tasks.head.state should be("RUNNING")
        }

        val httpRequests = mockServerClient.retrieveRecordedRequests(emsRequestForTable(emsTable))
        val parquetFile  = extractParquetFromRequest(httpRequests.head)
        fileAssertion(parquetFile)
      }
    }
  }
}
