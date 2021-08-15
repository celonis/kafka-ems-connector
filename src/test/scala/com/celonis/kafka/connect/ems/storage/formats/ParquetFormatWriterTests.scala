/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.StructSinkData
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.FileSystem
import com.celonis.kafka.connect.ems.storage.SampleData
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

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
      val sinkData     = StructSinkData(buildSimpleStruct())
      formatWriter.write(sinkData)
      formatWriter.close()
      formatWriter.size > 4 shouldBe true
    }
  }

}
