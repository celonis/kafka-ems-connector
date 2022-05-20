package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.testcontainers.scalatest.MockServerContainerPerSuite
import com.celonis.kafka.connect.ems.{randomEmsTable, randomTopicName}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

class EmsSinkTaskTest extends AnyFunSuite with MockServerContainerPerSuite with Matchers with SampleData {

  test("throws exception when config contains an invalid value") {
    val props = Map(
      "name"                                     -> "ems",
      EmsSinkConfigConstants.ENDPOINT_KEY        -> proxyServerUrl,
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> "target-table",
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "1",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000000L",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val task = new EmsSinkTask()

    a[ConnectException] should be thrownBy task.start(props)
  }

  test("writes to parquet format") {
    val connectorName = "ems"
    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()
    val props = Map(
      "name"                                     -> connectorName,
      "connector.class"                          -> "com.celonis.kafka.connect.ems.sink.EmsSinkConnector",
      "tasks.max"                                -> "1",
      "topics"                                   -> sourceTopic,
      EmsSinkConfigConstants.ENDPOINT_KEY        -> proxyServerUrl,
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> emsTable,
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "2",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000000",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val task = new EmsSinkTask()

    val user    = buildUserStruct("bob", "mr", 100.43)
    val records = List(toSinkRecord(sourceTopic, user, 0))

    task.start(props)
    task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records
    task.stop()

    val parquetFile  = new File(s"/tmp/$connectorName/$sourceTopic/1/1.parquet")

    eventually {
      assert(parquetFile.exists())
    }

    val reader       = parquetReader(parquetFile)
    val record       = reader.read()

    record.get("name").toString should be("bob")
    record.get("title").toString should be("mr")
    record.get("salary").asInstanceOf[Double] should be(100.43)
  }

}
