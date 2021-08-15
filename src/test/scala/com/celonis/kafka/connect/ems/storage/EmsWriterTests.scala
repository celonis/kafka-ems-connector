/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.SinkData
import com.celonis.kafka.connect.ems.model.StructSinkData
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.formats.FormatWriter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.io.File
import scala.concurrent.duration._

class EmsWriterTests extends AnyFunSuite with Matchers with MockitoSugar with SampleData {

  test("writes a records and updates the state") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(true)
    val startingRecords = 3L
    val startingSize    = 1000000L
    val emsWriter = new EmsWriter("sinkA",
                                  DefaultCommitPolicy(10000, 1000.minutes, 10000),
                                  formatWriter,
                                  WriterState(
                                    TopicPartition(new Topic("A"), new Partition(0)),
                                    new Offset(-1),
                                    None,
                                    startingRecords,
                                    startingRecords,
                                    System.currentTimeMillis(),
                                    simpleSchema,
                                    new File("abc"),
                                  ),
    )

    val struct = buildSimpleStruct()
    val record1 = Record(None,
                         StructSinkData(struct),
                         RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(10)),
    )

    val record2 = Record(None,
                         StructSinkData(struct),
                         RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(11)),
    )

    val firstSize = startingSize + 1002L
    when(formatWriter.size).thenReturn(firstSize)
    emsWriter.write(record1)
    var currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1
    currentState.fileSize shouldBe firstSize

    val secondSize = firstSize + 1002L
    when(formatWriter.size).thenReturn(secondSize)
    emsWriter.write(record2)
    currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1 + 1
    currentState.fileSize shouldBe secondSize
  }

  test("failing to write a record does not change the state") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(true)

    val expected = WriterState(
      TopicPartition(new Topic("A"), new Partition(0)),
      new Offset(-1),
      None,
      10,
      1,
      System.currentTimeMillis(),
      simpleSchema,
      new File("abc"),
    )
    val emsWriter = new EmsWriter("sinkA", DefaultCommitPolicy(10000, 1000.minutes, 10000), formatWriter, expected)

    val struct = buildSimpleStruct()
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

  test("writes only records with the offset greater than the last offset one") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(true)
    val startingRecords = 3L
    val startingSize    = 1000000L
    val emsWriter = new EmsWriter("sinkA",
                                  DefaultCommitPolicy(10000, 1000.minutes, 10000),
                                  formatWriter,
                                  WriterState(
                                    TopicPartition(new Topic("A"), new Partition(0)),
                                    new Offset(9),
                                    None,
                                    startingRecords,
                                    startingRecords,
                                    System.currentTimeMillis(),
                                    simpleSchema,
                                    new File("abc"),
                                  ),
    )

    val struct = buildSimpleStruct()
    val record1 = Record(None,
                         StructSinkData(struct),
                         RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(10)),
    )

    val firstSize = startingSize + 1002L
    when(formatWriter.size).thenReturn(firstSize)
    emsWriter.write(record1)
    var currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1
    currentState.fileSize shouldBe firstSize

    val secondSize = firstSize + 1002L
    when(formatWriter.size).thenReturn(secondSize)
    emsWriter.write(record1)
    currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1
    currentState.fileSize shouldBe firstSize

    val record2 = Record(None,
                         StructSinkData(struct),
                         RecordMetadata(TopicPartition(new Topic("a"), new Partition(0)), new Offset(9)),
    )

    emsWriter.write(record2)
    currentState = emsWriter.state
    currentState.records shouldBe startingRecords + 1
    currentState.fileSize shouldBe firstSize
  }

}
