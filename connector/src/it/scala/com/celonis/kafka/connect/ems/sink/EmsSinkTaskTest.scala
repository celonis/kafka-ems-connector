package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.parquet.parquetReader
import com.celonis.kafka.connect.ems.scalatest.fixtures.ems.withEmsSinkTask
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.testcontainers.scalatest.MockServerContainerPerSuite
import com.celonis.kafka.connect.transform.FlattenerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import java.io.File
import scala.jdk.CollectionConverters.SeqHasAsJava

class EmsSinkTaskTest extends AnyFunSuite with MockServerContainerPerSuite with Matchers with SampleData {

  test("writes to parquet format without obfuscation") {
    withEmsSinkTask(proxyServerUrl) { (connectorName, task, sourceTopic) =>
      val user    = buildUserStruct("bob", "mr", 100.43)
      val records = List(toSinkRecord(sourceTopic, user, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("name").toString should be("bob")
      record.get("title").toString should be("mr")
      record.get("salary").asInstanceOf[Double] should be(100.43)
    }
  }

  test("writes to parquet format using fixed characters to obfuscate string fields") {
    withEmsSinkTask(
      proxyServerUrl,
      commitRecords     = "2",
      sha512Salt        = Some("paprika"),
      obfuscationType   = Some("fix"),
      obfuscationFields = Some("name, title"),
    ) { (connectorName, task, sourceTopic) =>
      val user    = buildUserStruct("bob", "mr", 100.43)
      val records = List(toSinkRecord(sourceTopic, user, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("name").toString should be("*****")
      record.get("title").toString should be("*****")
      record.get("salary").asInstanceOf[Double] should be(100.43)
    }
  }

  test("writes to parquet format using SHA1 to obfuscate string fields") {
    withEmsSinkTask(
      proxyServerUrl,
      commitRecords     = "2",
      sha512Salt        = Some("turmeric"),
      obfuscationType   = Some("shA1"),
      obfuscationFields = Some("name, title"),
    ) { (connectorName, task, sourceTopic) =>
      val user    = buildUserStruct("bob", "mr", 100.43)
      val records = List(toSinkRecord(sourceTopic, user, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("name").toString should be("61584b76f6ece8fb9a328e7cf198094b2fac55c7")
      record.get("title").toString should be("fad7ad45e60072e88f86727c4b4adc9607de86e3")
      record.get("salary").asInstanceOf[Double] should be(100.43)
    }
  }

  test("writes to parquet format using SHA512 to obfuscate string fields") {
    withEmsSinkTask(
      proxyServerUrl,
      commitRecords     = "2",
      sha512Salt        = Some("pepper"),
      obfuscationType   = Some("shA512"),
      obfuscationFields = Some("name, title"),
    ) { (connectorName, task, sourceTopic) =>
      val user    = buildUserStruct("bob", "mr", 100.43)
      val records = List(toSinkRecord(sourceTopic, user, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("name").toString should be(
        "7dd03041fa190b49639a11f27c88dc5f8d9b2a8b747aa38e0dbd46d6bc896a0b42e194a40795d4f9e13ade218872e9a35c5072df1489c2b01398e73e09e22469",
      )
      record.get("title").toString should be(
        "514a50167f5186e629c8c03f3b99fafca0c9d35764b6e48d14e3e489abbe7504e0f53df0197c956b91425304e9c11e1c38c85e07d7183869899f5e746e25eb7b",
      )
      record.get("salary").asInstanceOf[Double] should be(100.43)
    }
  }

  test("flattens an avro record with explicitly given schema") {
    withEmsSinkTask(proxyServerUrl, flattenerConfig = Some(FlattenerConfig())) { (connectorName, task, sourceTopic) =>
      val userWithAddress = {
        val u      = new Struct(nestedUserSchema)
        val street = new Struct(streetSchema)
        street.put("name", "Surrey Road")
        street.put("number", 54)

        u.put("name", "Kev")
        u.put("title", "Senior Operative Manager")
        u.put("salary", 65.400)
        u.put("street", street)
        u
      }

      val records = List(toSinkRecord(sourceTopic, userWithAddress, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("name").toString should be("Kev")
      record.get("title").toString should be("Senior Operative Manager")
      record.get("salary").asInstanceOf[Double] should be(65.400)
      record.get("street_number") should be(54)
      record.get("street_name").toString should be("Surrey Road")
    }
  }

  test("flattens a nested JSON record with inferred schema") {
    withEmsSinkTask(proxyServerUrl, flattenerConfig = Some(FlattenerConfig())) { (connectorName, task, sourceTopic) =>
      val jsonValue = Map[String, Any](
        "nested" -> Map[String, Any]("a_bool" -> true, "an_int" -> 3).asJava,
      ).asJava

      val records = List(
        new SinkRecord(sourceTopic, 1, null, null, null, jsonValue, 0),
      )

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      record.get("nested_a_bool").asInstanceOf[Boolean] should be(true)
      record.get("nested_an_int").asInstanceOf[Int] should be(3)
    }
  }

  test("execute flattener with jsonblob chunking") {
    val flattenConfig = FlattenerConfig(jsonBlobChunks = Some(FlattenerConfig.JsonBlobChunks(1, 5000)))
    withEmsSinkTask(proxyServerUrl, flattenerConfig = Some(flattenConfig)) { (connectorName, task, sourceTopic) =>
      val jsonValue = Map[String, Any](
        "some" -> Map[String, Any]("json" -> true).asJava,
      ).asJava

      val records = List(
        new SinkRecord(sourceTopic, 1, null, null, null, jsonValue, 0),
      )

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)
      task.put(records.asJava)
      task.close(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava) // writes records

      val file   = waitForParquetFile(connectorName, sourceTopic)
      val record = parquetReader(file).read()

      val jsonPayload = record.get("payload_chunk1").toString
      val om          = new ObjectMapper()
      val parsedChunk = om.readValue(jsonPayload, classOf[java.util.Map[String, Any]])

      parsedChunk shouldEqual jsonValue
    }
  }

  ignore("throws an exception attempting to obfuscate non-string fields") {
    withEmsSinkTask(
      proxyServerUrl,
      commitRecords     = "2",
      sha512Salt        = Some("cumin"),
      obfuscationType   = Some("fix"),
      obfuscationFields = Some("name, salary"),
    ) { (_, task, sourceTopic) =>
      val user    = buildUserStruct("bob", "mr", 100.43)
      val records = List(toSinkRecord(sourceTopic, user, 0))

      task.open(Seq(new KafkaTopicPartition(sourceTopic, 1)).asJava)

      a[Throwable] should be thrownBy task.put(records.asJava)
    }
  }

  private def waitForParquetFile(connectorName: String, sourceTopic: String): File = {
    log.info("WAITING FOR PARQUET FILE")
    val file = new File(s"/tmp/$connectorName/$sourceTopic/1/1.parquet")
    eventually {
      assert(file.exists())
    }
    file
  }

}
