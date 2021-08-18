/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats
import com.celonis.kafka.connect.ems.conversion.ValueConverter
import com.celonis.kafka.connect.ems.data.ComplexObject
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.FileSystem
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
import io.circe.syntax.EncoderOps
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ParquetFormatWriterTests extends AnyFunSuite with Matchers with WorkingDirectory with SampleData {
  test("rollover on schema change returns true") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("B"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)
      output.size shouldBe 0L
      val formatWriter = ParquetFormatWriter.from(output, simpleSchema)
      formatWriter.rolloverFileOnSchemaChange() shouldBe true
      formatWriter.close()
    }
  }

  test("returns 0 on file size after initialization") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("C"), new Partition(1))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)
      output.size shouldBe 0L
      val formatWriter = ParquetFormatWriter.from(output, simpleSchema)
      formatWriter.size shouldBe 4L //4 BYTES from MAGIC PAR1
    }
  }

  test("writes a struct to a file and returns the new non-zero file size") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)
      output.size shouldBe output.outputFile().length()
      val formatWriter = ParquetFormatWriter.from(output, simpleSchema)
      val struct       = buildSimpleStruct()
      formatWriter.write(struct)
      formatWriter.close()
      formatWriter.size > 4 shouldBe true
    }
  }

  test("writes a JSON schemaless to the file") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)

      val converter = new org.apache.kafka.connect.json.JsonConverter()
      converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                              "converter.type"                          -> "value",
                              "schemas.enable"                          -> "false",
      ).asJava)
      val entry =
        ComplexObject(8,
                      8,
                      8,
                      8,
                      8.8f,
                      8.8,
                      true,
                      "foo",
                      "foo".getBytes(),
                      List("a", "b", "c"),
                      Map("key" -> 1),
                      Map("key" -> 1),
        )
      val schemaAndValue = converter.toConnectData("topic", entry.asJson.noSpaces.getBytes)

      val struct       = ValueConverter.apply(schemaAndValue.value()).getOrElse(fail("Should convert the map"))
      val formatWriter = ParquetFormatWriter.from(output, struct.schema())
      formatWriter.write(struct)
      formatWriter.close()
      println(formatWriter.size)
      formatWriter.size > 4 shouldBe true
    }
  }

}
