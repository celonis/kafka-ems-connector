/*
 * Copyright 2023 Celonis SE
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

package com.celonis.kafka.connect.ems.storage

import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.implicits.global
import cats.syntax.option._
import com.celonis.kafka.connect.ems.config.ExplodeConfig
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.model._
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.file.Files
import java.nio.file.Path

class WriterManagerTests extends AnyFunSuite with Matchers with WorkingDirectory with MockitoSugar with SampleData {
  val fsOps = new FileSystemOperations(fsImpl)

  test("cleans up on topic partitions allocated") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(2))
      val tp3      = TopicPartition(new Topic("B"), new Partition(3))
      val output1  = fsOps.createOutput(dir, sink, tp1)
      val output2  = fsOps.createOutput(dir, sink, tp2)
      val output3  = fsOps.createOutput(dir, sink, tp3)
      val uploader = mock[Uploader[IO]]
      val builder  = mock[WriterBuilder]
      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)
      manager.open(Set(tp1, tp3)).unsafeRunSync()
      Files.exists(output1.outputFile()) shouldBe false
      Files.exists(output2.outputFile()) shouldBe true
      Files.exists(output3.outputFile()) shouldBe false
    }
  }

  test("opens a writer on the first record for the topic partition") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val uploader = mock[Uploader[IO]]
      val builder  = mock[WriterBuilder]
      val writer   = mock[Writer]
      when(builder.writerFrom(any[Record])).thenReturn(writer)

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val uploader = mock[Uploader[IO]]
      val builder  = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp2, new Offset(11)))

      val writer1 = mock[Writer]
      when(builder.writerFrom(record1)).thenReturn(writer1)
      val writer2 = mock[Writer]
      when(builder.writerFrom(record2)).thenReturn(writer2)

      when(writer1.shouldFlush).thenReturn(false)
      when(writer2.shouldFlush).thenReturn(false)

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
      } yield ()).unsafeRunSync()

      verify(builder, times(1)).writerFrom(record1)
      verify(builder, times(1)).writerFrom(record2)
      verify(writer1, times(1)).write(record1)
      verify(writer2, times(1)).write(record2)

      // trigger the upload if applicable
      manager.maybeUploadData.unsafeRunSync()
      verifyNoInteractions(uploader)
    }
  }

  test("returns an empty Map when there's no committed offset") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder  = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()
      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(tp1, None, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1),
      )
      when(writer2.state).thenReturn(
        WriterState(tp2, None, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer3.state).thenReturn(
        WriterState(tp3, None, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3),
      )

      manager.preCommit(Map(
        tp1 -> new OffsetAndMetadata(100),
        tp2 -> new OffsetAndMetadata(210),
        tp3 -> new OffsetAndMetadata(82),
      )).unsafeRunSync() shouldBe Map.empty
    }
  }

  test("returns the latest committed offsets") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      val builder  = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(
          tp1,
          record1.metadata.offset.some,
          record1.metadata.offset,
          new Offset(99).some,
          0,
          1,
          1,
          simpleSchemaV1,
          file1,
        ),
      )
      when(writer2.state).thenReturn(
        WriterState(
          tp2,
          record2.metadata.offset.some,
          record2.metadata.offset,
          new Offset(111).some,
          0,
          1,
          1,
          simpleSchemaV1,
          file2,
        ),
      )
      when(writer3.state).thenReturn(
        WriterState(
          tp3,
          record3.metadata.offset.some,
          record3.metadata.offset,
          new Offset(81).some,
          0,
          1,
          1,
          simpleSchemaV1,
          file3,
        ),
      )

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
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      val builder = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(tp1, record1.metadata.offset.some, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1),
      )
      when(writer2.state).thenReturn(
        WriterState(tp2, record2.metadata.offset.some, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer3.state).thenReturn(
        WriterState(tp3, record3.metadata.offset.some, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3),
      )

      when(writer2.shouldFlush).thenReturn(true)
      when(uploader.upload(UploadRequest.fromWriterState(writer2.state))).thenReturn(IO.pure(EmsUploadResponse(
        "1",
        file2.getFileName.toString,
        "b1",
        "NEW",
        "c1".some,
        None,
        None,
      )))
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(UploadRequest.fromWriterState(writer2.state))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2, times(1)).close()
    }
  }

  test("keep the file once it has been uploaded") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      when(uploader.getOrderFieldName).thenReturn("int_field".some)

      val builder = mock[WriterBuilder]

      val manager = new WriterManager[IO](sink,
                                          uploader,
                                          dir,
                                          builder,
                                          Ref.unsafe(Map.empty),
                                          new ParquetFileCleanupRename(fsImpl),
                                          fsOps,
      )

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(tp1, record1.metadata.offset.some, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1),
      )
      when(writer2.state).thenReturn(
        WriterState(tp2, record2.metadata.offset.some, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer3.state).thenReturn(
        WriterState(tp3, record3.metadata.offset.some, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3),
      )

      when(writer2.shouldFlush).thenReturn(true)
      when(
        uploader.upload(UploadRequest.fromWriterState(writer2.state)),
      ).thenReturn(IO.pure(EmsUploadResponse("1", file2.getFileName.toString, "b1", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      val retainedFile = new ParquetFileCleanupRename(fsImpl).renamedFile(file2, record2.metadata.offset)
      Files.exists(retainedFile) shouldBe true
      verify(uploader, times(1)).upload(UploadRequest.fromWriterState(writer2.state))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2, times(1)).close()
    }
  }

  test("commit data when there is a schema change") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      val builder = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(tp1, record1.metadata.offset.some, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1),
      )
      when(writer2.state).thenReturn(
        WriterState(tp2, record2.metadata.offset.some, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer3.state).thenReturn(
        WriterState(tp3, record3.metadata.offset.some, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3),
      )
      verify(builder, times(1)).writerFrom(record2)
      reset(writer2)
      when(writer2.state).thenReturn(
        WriterState(tp2, record2.metadata.offset.some, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer2.shouldRollover(any[Schema])).thenReturn(true)
      val writer2Next = mock[Writer]
      reset(builder)
      when(builder.writerFrom(writer2, record2)).thenReturn(writer2Next)
      when(
        uploader.upload(UploadRequest.fromWriterState(writer2.state)),
      ).thenReturn(IO(EmsUploadResponse("1", file2.getFileName.toString, "b", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(UploadRequest.fromWriterState(writer2.state))
      verify(builder, times(1)).writerFrom(writer2, record2)
      verify(writer2, times(0)).write(record2)
      verify(writer2Next, times(1)).write(record2)
    }
  }

  test("commit data when there is a schema change after a flush (i.e. when current writer state is empty)") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("topic"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      val builder = mock[WriterBuilder]

      val file1 = createEmptyFile(dir, "abc1")
      def simpleWriter(schema: Schema) = new SimpleDummyWriter(2, file1, schema, tp1)

      val manager =
        new WriterManager[IO](
          sink,
          uploader,
          dir,
          builder,
          // make sure current writer has a different schema and 0 records
          Ref.unsafe(Map(tp1 -> simpleWriter(simpleSchemaV2))),
          ParquetFileCleanupDelete,
          fsOps,
        )

      val struct  = buildSimpleStruct()
      val record1 = Record(struct, RecordMetadata(tp1, new Offset(10)))
      val record2 = Record(struct, RecordMetadata(tp1, new Offset(11)))

      // Repeated here because they must not be the same instance
      when(builder.writerFrom(any[Record])).thenReturn(
        simpleWriter(simpleSchemaV1),
        simpleWriter(simpleSchemaV1),
        simpleWriter(simpleSchemaV1),
        simpleWriter(simpleSchemaV1),
      )

      when(builder.writerFrom(any[Writer], any[Record])).thenReturn(simpleWriter(simpleSchemaV1))

      when(uploader.upload(any[UploadRequest]))
        .thenReturn(IO(EmsUploadResponse("1", file1.getFileName.toString, "b", "NEW", "c1".some, None, None)))

      manager.write(record1).unsafeRunSync()
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(any[UploadRequest])
    }
  }

  test("the uploader throws an exception the state is not corrupted on attempting to write again") {
    withDir { dir =>
      val sink     = "sinkA"
      val tp1      = TopicPartition(new Topic("A"), new Partition(1))
      val tp2      = TopicPartition(new Topic("B"), new Partition(3))
      val tp3      = TopicPartition(new Topic("C"), new Partition(0))
      val uploader = mock[Uploader[IO]]
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      val builder = mock[WriterBuilder]

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      val struct  = buildSimpleStruct()
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

      // open the writers
      (for {
        _ <- manager.write(record1)
        _ <- manager.write(record2)
        _ <- manager.write(record3)
      } yield ()).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      val file3 = createEmptyFile(dir, "abc3")
      when(writer1.state).thenReturn(
        WriterState(tp1, record1.metadata.offset.some, record1.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file1),
      )
      when(writer2.state).thenReturn(
        WriterState(tp2, record2.metadata.offset.some, record2.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file2),
      )
      when(writer3.state).thenReturn(
        WriterState(tp3, record3.metadata.offset.some, record3.metadata.offset, None, 0, 1, 1, simpleSchemaV1, file3),
      )

      when(writer2.shouldFlush).thenReturn(true)
      val exception = new RuntimeException("Just throwing")
      when(
        uploader.upload(UploadRequest.fromWriterState(writer2.state)),
      ).thenReturn(IO.raiseError(exception))
      val ex = the[RuntimeException] thrownBy manager.write(record2).unsafeRunSync()
      ex shouldBe exception
      verify(builder, times(0)).writerFrom(writer2)
      reset(uploader)
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      when(
        uploader.upload(UploadRequest.fromWriterState(writer2.state)),
      ).thenReturn(IO(EmsUploadResponse("1", file2.getFileName.toString, "b", "NEW", "c1".some, None, None)))
      val writer2Next = mock[Writer]
      when(builder.writerFrom(writer2)).thenReturn(writer2Next)
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(UploadRequest.fromWriterState(writer2.state))
      verify(builder, times(1)).writerFrom(writer2)
      verify(writer2Next, times(0)).write(record2)
    }
  }

  test("handle schema change when a column is dropped") {
    withDir { dir =>
      val sink           = "sinkA"
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
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      val builder = new WriterBuilderImpl(dir,
                                          sink,
                                          new DefaultCommitPolicy(10000, 10000, 2),
                                          ParquetConfig(10, ParquetFileCleanupDelete),
                                          ExplodeConfig(None),
                                          fsOps,
      )

      val manager =
        new WriterManager[IO](sink, uploader, dir, builder, Ref.unsafe(Map.empty), ParquetFileCleanupDelete, fsOps)

      // open the writers
      manager.write(record1).unsafeRunSync()

      val file1 = createEmptyFile(dir, "abc1")
      val file2 = createEmptyFile(dir, "abc2")
      when(
        uploader.upload(
          UploadRequest(
            any[Path](),
            s"${topicPartition.topic.value}_${topicPartition.partition.value}_${record2.metadata.offset.value}",
          ),
        ),
      ).thenReturn(IO(EmsUploadResponse("1", file1.getFileName.toString, "b", "NEW", "c1".some, None, None)))
      manager.write(record2).unsafeRunSync()

      verify(uploader, times(1)).upload(
        UploadRequest(
          any[Path](),
          s"${topicPartition.topic.value}_${topicPartition.partition.value}_${record2.metadata.offset.value}",
        ),
      )

      reset(uploader)
      when(uploader.getOrderFieldName).thenReturn("int_field".some)
      when(
        uploader.upload(
          UploadRequest(
            any[Path](),
            s"${topicPartition.topic.value}_${topicPartition.partition.value}_${record3.metadata.offset.value}",
          ),
        ),
      ).thenReturn(IO(EmsUploadResponse("2", file2.getFileName.toString, "b", "NEW", "c1".some, None, None)))

      manager.write(record3).unsafeRunSync()
      verify(uploader, times(1)).upload(
        UploadRequest(
          any[Path](),
          s"${topicPartition.topic.value}_${topicPartition.partition.value}_${record3.metadata.offset.value}",
        ),
      )
    }
  }

  private def createEmptyFile(baseDir: Path, fileName: String): Path = {
    val path = baseDir.resolve(fileName)
    Files.createFile(path)
    path
  }

  /** A simple writer that just update the number of records at every write
    */
  class SimpleDummyWriter(maxRecords: Int, file: Path, baseSchema: Schema, topicPartition: TopicPartition)
      extends Writer {
    private var records: Long = 0

    override def shouldFlush: Boolean = records >= maxRecords

    override def write(record: Record): Unit =
      records += 1

    override def shouldRollover(schema: Schema): Boolean = schema != baseSchema

    override def state: WriterState = WriterState(
      topicPartition,
      Some(new Offset(0)),
      new Offset(0),
      None,
      0,
      records,
      1,
      simpleSchemaV1,
      file,
    )

    override def close(): Unit = ()
  }

}
