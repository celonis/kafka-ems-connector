/*
 * Copyright 2022 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.ems.storage.formats
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.conversion.DataConverter
import com.celonis.kafka.connect.ems.data.ComplexObject
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.FileSystem
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupDelete
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
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
      val formatWriter =
        ParquetFormatWriter.from(output, simpleSchemaV1, ParquetConfig.Default, new NoOpExploder().explode)
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
      val formatWriter =
        ParquetFormatWriter.from(output, simpleSchemaV1, ParquetConfig.Default, new NoOpExploder().explode)
      formatWriter.size shouldBe 4L // 4 BYTES from MAGIC PAR1
    }
  }

  test("writes a struct to a file and returns the new non-zero file size") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)
      output.size shouldBe output.outputFile().length()
      val formatWriter =
        ParquetFormatWriter.from(output, simpleSchemaV1, ParquetConfig.Default, new NoOpExploder().explode)
      val struct = buildSimpleStruct()
      formatWriter.write(struct)
      formatWriter.close()
      formatWriter.size > 4 shouldBe true
    }
  }

  test("writes a schemaless JSON") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)

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
      // Schemaless json go through normalisation
      val normalisedValue = InferSchemaAndNormaliseValue(schemaAndValue.value()).get.normalisedValue
      val struct          = DataConverter.apply(normalisedValue).getOrElse(fail("Should convert the map"))
      val formatWriter =
        ParquetFormatWriter.from(output, struct.getSchema, ParquetConfig.Default, new NoOpExploder().explode)
      formatWriter.write(struct)
      formatWriter.close()
      formatWriter.size > 4 shouldBe true
    }
  }

  private val converter = new org.apache.kafka.connect.json.JsonConverter()
  converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                          "converter.type"                          -> "value",
                          "schemas.enable"                          -> "false",
  ).asJava)

  test("flush to disk after N records") {
    withDir { dir =>
      val sinkName       = "sA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(2))
      val output         = FileSystem.createOutput(dir, sinkName, topicPartition)
      output.size shouldBe output.outputFile().length()
      val count = 100
      val formatWriter = ParquetFormatWriter.from(output,
                                                  simpleSchemaV1,
                                                  ParquetConfig(count, ParquetFileCleanupDelete),
                                                  new NoOpExploder().explode,
      )
      (1 until count).foreach { _ =>
        formatWriter.write(buildSimpleStruct())
      }
      val beforeFlush = formatWriter.size
      formatWriter.write(buildSimpleStruct())
      val afterFlush = formatWriter.size
      afterFlush shouldBe >(beforeFlush)
      formatWriter.close()
    }
  }
}
