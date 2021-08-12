/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.storage
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.SinkData
import com.celonis.kafka.connect.ems.model.StructSinkData
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.EmsWriter
import com.celonis.kafka.connect.ems.storage.formats.FormatWriter
import com.celonis.kafka.connect.ems.storage.WriterState
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.io.File
import scala.concurrent.duration._

class EmsWriterTests extends AnyFunSuite with Matchers with MockitoSugar {
  private val schema = SchemaBuilder.struct.name("record")
    .version(1)
    .field("id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .build

  test("writes a records and updates the state") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(true)

    val startingRecords = 3L
    val startingSize    = 1000000L
    val emsWriter = new EmsWriter("sinkA",
                                  DefaultCommitPolicy(10000, 1000.minutes, 10000),
                                  formatWriter,
                                  WriterState(
                                    new Topic("A"),
                                    new Partition(0),
                                    new Offset(-1),
                                    None,
                                    startingRecords,
                                    startingRecords,
                                    System.currentTimeMillis(),
                                    schema,
                                    new File("abc"),
                                  ),
    )

    val struct = new Struct(schema)
    val record = Record(None,
                        StructSinkData(struct),
                        RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(10)),
    )

    val firstSize = startingSize + 1002L
    when(formatWriter.size).thenReturn(firstSize)
    emsWriter.write(record)
    var currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1
    currentState.fileSize shouldBe firstSize

    val secondSize = firstSize + 1002L
    when(formatWriter.size).thenReturn(secondSize)
    emsWriter.write(record)
    currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1 + 1
    currentState.fileSize shouldBe secondSize
  }

  test("failing to write a record does not change the state") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(true)

    val expected = WriterState(
      new Topic("A"),
      new Partition(0),
      new Offset(-1),
      None,
      10,
      1,
      System.currentTimeMillis(),
      schema,
      new File("abc"),
    )
    val emsWriter = new EmsWriter("sinkA", DefaultCommitPolicy(10000, 1000.minutes, 10000), formatWriter, expected)

    val struct = new Struct(schema)
    val record = Record(None,
                        StructSinkData(struct),
                        RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(10)),
    )

    val ex = new RuntimeException("throwing")
    when(formatWriter.write(any[SinkData])).thenThrow(ex)
    val actualExceptions = the[RuntimeException] thrownBy emsWriter.write(record)
    actualExceptions shouldBe ex
    emsWriter.state shouldBe expected
  }
}
