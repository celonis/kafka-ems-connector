/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.syntax.option._
import com.celonis.kafka.connect.ems.config.{ExplodeConfig, ParquetConfig}
import com.celonis.kafka.connect.ems.model._
import org.apache.avro.generic.GenericData
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.io.File
import java.nio.file.Paths

class WriterManagerTests extends AnyFunSuite with Matchers with WorkingDirectory with MockitoSugar with SampleData {
  test("cleans up on topic partitions allocated") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(2))
      val tp3 = TopicPartition(new Topic("B"), new Partition(3))
      val output1 = FileSystem.createOutput(dir, sink, tp1)
      val output2 = FileSystem.createOutput(dir, sink, tp2)
      val output3 = FileSystem.createOutput(dir, sink, tp3)
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]
      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)
      manager.open(Set(tp1, tp3)).unsafeRunSync()
      output1.outputFile().exists() shouldBe false
      output2.outputFile().exists() shouldBe true
      output3.outputFile().exists() shouldBe false
    }
  }

  test("opens a writer on the first record for the topic partition") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]
      val writer = mock[Writer]
      when(builder.writerFrom(any[Record])).thenReturn(writer)

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp1, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp2, new Offset(24)))

      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()
      verify(builder, times(1)).writerFrom(record1)
      verify(builder, times(0)).writerFrom(record2)
      verify(builder, times(1)).writerFrom(record3)
      verify(builder, times(0)).writerFrom(any[Writer])

      verify(writer, times(3)).write(any[Record])
    }
  }

  test("does not upload data if the writers have nothing to flush") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
      } yield ()).unsafeRunSync()

      verify(builder, times(1)).writerFrom(record1)
      verify(builder, times(1)).writerFrom(record2)
      verify(writer1, times(1)).write(record1)
      verify(writer2, times(1)).write(record2)

      //trigger the upload if applicable
      manager.maybeUploadData().unsafeRunSync()
      verifyNoInteractions(uploader)
    }
  }

  test("returns an empty Map when there's no committed offset") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)

      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()
      val file1 = new File("abc1")
      val file2 = new File("abc2")
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1))
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer3.state).thenReturn(WriterState(tp3, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3))

      manager.preCommit(Map(
        tp1 -> new OffsetAndMetadata(100),
        tp2 -> new OffsetAndMetadata(210),
        tp3 -> new OffsetAndMetadata(82),
      )).unsafeRunSync() shouldBe Map.empty
    }
  }

  test("returns the latest committed offsets") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)

      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = new File("abc2")
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1,
        record1.metadata.offset,
        new Offset(99).some,
        0,
        1,
        1,
        simpleSchemaV1,
        file1,
      ))
      when(writer2.state).thenReturn(WriterState(tp2,
        record2.metadata.offset,
        new Offset(111).some,
        0,
        1,
        1,
        simpleSchemaV1,
        file2,
      ))
      when(writer3.state).thenReturn(WriterState(tp3,
        record3.metadata.offset,
        new Offset(81).some,
        0,
        1,
        1,
        simpleSchemaV1,
        file3,
      ))

      manager.preCommit(Map(
        tp1 -> new OffsetAndMetadata(100),
        tp2 -> new OffsetAndMetadata(210),
        tp3 -> new OffsetAndMetadata(82),
      )).unsafeRunSync() shouldBe Map(
        tp1 -> new OffsetAndMetadata(99),
        tp2 -> new OffsetAndMetadata(111),
        tp3 -> new OffsetAndMetadata(81),
      )
    }
  }

  test("commit data when enough data has been accumulated") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)
      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = new File("abc2")
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1))
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer3.state).thenReturn(WriterState(tp3, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3))

      when(writer2.shouldFlush).thenReturn(true)
      when(
        uploader.upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        )),
      ).thenReturn(IO.pure(EmsUploadResponse("1", file2.getName, "b1", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1))
        .upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        ))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2, times(1)).close()
    }
  }

  test("keep the file once it has been uploaded") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupRename)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)
      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = Paths.get(dir.toString, "abc2").toFile
      file2.createNewFile() shouldBe true
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1))
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer3.state).thenReturn(WriterState(tp3, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3))

      when(writer2.shouldFlush).thenReturn(true)
      when(
        uploader.upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        )),
      ).thenReturn(IO.pure(EmsUploadResponse("1", file2.getName, "b1", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      val retainedFile = ParquetFileCleanupRename.renamedFile(file2, record2.metadata.offset)
      retainedFile.toFile.exists() shouldBe true
      verify(uploader, times(1)).upload(UploadRequest(file2,
        writer2.state.topicPartition.topic,
        writer2.state.topicPartition.partition,
        writer2.state.offset,
      ))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2, times(1)).close()
    }
  }

  test("commit data when there is a schema change") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)
      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = new File("abc2")
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1))
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer3.state).thenReturn(WriterState(tp3, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3))
      verify(builder, times(1)).writerFrom(record2)
      reset(writer2)
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer2.shouldRollover(any[Schema])).thenReturn(true)
      val writer2Next = mock[Writer]
      reset(builder)
      when(builder.writerFrom(record2)).thenReturn(writer2Next)
      when(
        uploader.upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        )),
      ).thenReturn(IO(EmsUploadResponse("1", file2.getName, "b", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(UploadRequest(file2,
        writer2.state.topicPartition.topic,
        writer2.state.topicPartition.partition,
        writer2.state.offset,
      ))
      verify(builder, times(1)).writerFrom(record2)
      verify(writer2, times(0)).write(record2)
      verify(writer2Next, times(1)).write(record2)
    }
  }

  test("the uploader throws an exception the state is not corrupted on attempting to write again") {
    withDir { dir =>
      val sink = "sinkA"
      val tp1 = TopicPartition(new Topic("A"), new Partition(1))
      val tp2 = TopicPartition(new Topic("B"), new Partition(3))
      val tp3 = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      val struct = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))
      val record3 = Record(struct, RecordMetadata(tp3, new Offset(91)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)
      val writer3 = mock[Writer]
      when(builder.writerFrom(record3)).thenReturn(writer3)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)
      when(writer3.shouldFlush).thenReturn(false)

      //open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = new File("abc2")
      val file3 = new File("abc3")
      when(writer1.state).thenReturn(WriterState(tp1, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1))
      when(writer2.state).thenReturn(WriterState(tp2, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2))
      when(writer3.state).thenReturn(WriterState(tp3, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3))

      when(writer2.shouldFlush).thenReturn(true)
      val exception = new RuntimeException("Just throwing")
      when(
        uploader.upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        )),
      ).thenReturn(IO.raiseError(exception))
      val ex = the[RuntimeException] thrownBy manager.write(record2).unsafeRunSync()
      ex shouldBe exception
      verify(builder, times(0)).writerFrom(writer2)
      reset(uploader)
      when(
        uploader.upload(UploadRequest(file2,
          writer2.state.topicPartition.topic,
          writer2.state.topicPartition.partition,
          writer2.state.offset,
        )),
      ).thenReturn(IO(EmsUploadResponse("1", file2.getName, "b", "NEW", "c1".some, None, None)))
      val writer2Next = mock[Writer]
      when(builder.writerFrom(writer2)).thenReturn(writer2Next)
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(UploadRequest(file2,
        writer2.state.topicPartition.topic,
        writer2.state.topicPartition.partition,
        writer2.state.offset,
      ))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2Next, times(0)).write(record2)
    }
  }

  test("handle schema change when a column is dropped") {
    withDir { dir =>
      val sink = "sinkA"
      val topicPartition = TopicPartition(new Topic("A"), new Partition(0))

      val schema_v1 = SchemaBuilder.record("record").fields()
        .requiredString("name")
        .requiredLong("count")
        .requiredBoolean("flag")
        .endRecord()

      val struct1 = new GenericData.Record(schema_v1)
      struct1.put("name", "string")
      struct1.put("count", 999L)
      struct1.put("flag", false)

      val schema_v2 = SchemaBuilder.record("record").fields()
        .requiredLong("count")
        .requiredBoolean("flag")
        .endRecord()

      val struct2 = new GenericData.Record(schema_v2)
      struct2.put("count", 1000)
      struct2.put("flag", false)

      val struct3 = new GenericData.Record(schema_v2)
      struct3.put("count", 2000)
      struct3.put("flag", true)

      val record1 = Record(struct1, RecordMetadata(topicPartition, new Offset(10)))
      val record2 = Record(struct2, RecordMetadata(topicPartition, new Offset(11)))
      val record3 = Record(struct3, RecordMetadata(topicPartition, new Offset(12)))

      val uploader = mock[Uploader[IO]]
      val builder = new WriterBuilderImpl(dir, sink, DefaultCommitPolicy(10000, 10000, 2), ParquetConfig(10, ParquetFileCleanupDelete), ExplodeConfig(None))

      val manager = new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete)

      //open the writers
      manager.write(record1).unsafeRunSync()

      val file1 = new File("abc1")
      val file2 = new File("abc2")
      when(
        uploader.upload(UploadRequest(any[File](),
          topicPartition.topic,
          topicPartition.partition,
          record1.metadata.offset,
        )),
      ).thenReturn(IO(EmsUploadResponse("1", file1.getName, "b", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()


      verify(uploader, times(1)).upload(UploadRequest(any[File](),
        topicPartition.topic,
        topicPartition.partition,
        record1.metadata.offset,
      ))

reset(uploader)
      when(
        uploader.upload(UploadRequest(any[File](),
          topicPartition.topic,
          topicPartition.partition,
          record3.metadata.offset,
        )),
      ).thenReturn(IO(EmsUploadResponse("2", file2.getName, "b", "NEW", "c1".some, None, None)))

      manager.write(record3).unsafeRunSync()
      verify(uploader, times(1)).upload(UploadRequest(any[File](),
        topicPartition.topic,
        topicPartition.partition,
        record3.metadata.offset,
      ))
    }
  }
}
