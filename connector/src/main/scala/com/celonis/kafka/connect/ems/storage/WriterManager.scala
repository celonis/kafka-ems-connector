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

package com.celonis.kafka.connect.ems.storage

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.model.TopicPartitionOffset
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.OffsetAndMetadata

import java.nio.file.Path

/** Manages the lifecycle of [[Writer]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore it is convenient to extract this to
  * another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent sinks, since file handles cannot
  * be safely shared without considerable overhead.
  */
class WriterManager[F[_]](
  sinkName:      String,
  uploader:      Uploader[F],
  workingDir:    Path,
  writerBuilder: WriterBuilder,
  writersRef:    Ref[F, Map[TopicPartition, Writer]],
  fileCleanup:   ParquetFileCleanup,
)(
  implicit
  A: Async[F],
) extends StrictLogging {

  /** Uploads the data to EMS if the commit policy is met.
    * @return
    */
  def maybeUploadData(): F[Unit] = {
    // The data is uploaded sequentially. We might want to parallelize the process
    logger.debug(s"[{}] Received call to WriterManager.maybeUploadData", sinkName)
    for {
      writers <- writersRef.get.map(_.values.filter(_.shouldFlush).toList)
      _       <- writers.traverse(w => commit(w, writerBuilder.writerFrom(w)))
    } yield ()
  }

  private case class CommitWriterResult(newWriter: Writer, offset: TopicPartitionOffset)

  /** Committing a file created by the writer, will result creating a new writer
    * @param writer
    *   \- An instance of [[Writer]]
    * @return
    */
  private def commit(writer: Writer, buildFn: => Writer): F[Option[CommitWriterResult]] = {
    val state = writer.state
    // check if data was written to the file
    if (state.records == 0) A.pure(None)
    else {
      for {
        _   <- A.delay(writer.close())
        file = writer.state.file
        _ <- A.delay(
          logger.info(
            s"Uploading file:$file size:${file.length()} for topic-partition:${TopicPartition.show.show(state.topicPartition)} and offset:${state.offset.show}",
          ),
        )
        uploadRequest = UploadRequest(file, state.topicPartition.topic, state.topicPartition.partition, state.offset)
        _            <- A.delay(println(s"Request:${UploadRequest.show.show(uploadRequest)}"))
        response     <- uploader.upload(uploadRequest)
        _            <- A.delay(logger.info(s"Received ${response.asJson.noSpaces} for uploading file:$file"))
        _            <- A.delay(fileCleanup.clean(file, state.offset))
        newWriter    <- A.delay(buildFn)
        _            <- A.delay(logger.debug("Creating a new writer for [{}]", writer.state.show))
        _            <- writersRef.update(map => map + (writer.state.topicPartition -> newWriter))
      } yield CommitWriterResult(
        newWriter,
        TopicPartitionOffset(writer.state.topicPartition.topic,
                             writer.state.topicPartition.partition,
                             writer.state.offset,
        ),
      ).some
    }
  }

  /** When a partition is opened we cleanup the folder where the temp files are accumulating
    * @param partitions
    *   \- A set of topic-partition tuples which the current task will own
    */
  def open(partitions: Set[TopicPartition]): F[Unit] =
    for {
      writers <- writersRef.get.map(_.values.toList)
      _       <- writers.traverse(w => A.delay(w.close()))
      _       <- writersRef.update(_ => Map.empty)
      _       <- partitions.toList.traverse(partition => A.delay(FileSystem.cleanup(workingDir, sinkName, partition)))
    } yield ()

  def close(partitions: List[TopicPartition]): F[Unit] =
    for {
      _ <- A.delay(logger.info(s"[{}] Received call to WriterManager.close(partitions):",
                               sinkName,
                               partitions.map(_.show).mkString(";"),
      ))
      writersMap   <- writersRef.get
      _            <- partitions.flatMap(writersMap.get).traverse(w => A.delay(w.close()))
      newWritersMap = writersMap -- partitions.toSet
      // do not cleanup the folders. on open partitions we do that.
      _ <- writersRef.update(_ => newWritersMap)
    } yield ()

  def close(): F[Unit] =
    for {
      _          <- A.delay(logger.info(s"[{}] Received call to WriterManager.close()", sinkName))
      writers    <- writersRef.get.map(_.values.toList)
      partitions <- writers.traverse(w => A.delay(w.close()).map(_ => w.state.topicPartition))
      _          <- partitions.traverse(partition => A.delay(FileSystem.cleanup(workingDir, sinkName, partition)))
      _          <- writersRef.update(_ => Map.empty)
    } yield ()

  def write(record: Record): F[Unit] =
    for {
      _ <- A.delay {
        logger.debug(
          s"[$sinkName] Received call to WriterManager.write for ${RecordMetadata.show.show(record.metadata)}",
        )
      }
      writersMap <- writersRef.get
      writer <- writersMap.get(record.metadata.topicPartition) match {
        case Some(value) => A.pure(value)
        case None =>
          A.pure(writerBuilder.writerFrom(record)).flatMap { writer =>
            writersRef.update(map => map + (record.metadata.topicPartition -> writer))
              .map(_ => writer)
          }
      }
      schema = record.value.getSchema
      latestWriter <- {
        if (writer.shouldRollover(schema))
          commit(writer, writerBuilder.writerFrom(record)).map(_.fold(writerBuilder.writerFrom(record))(_.newWriter))
        else A.pure(writer)
      }
      _ <- A.delay(latestWriter.write(record))
      _ <- if (latestWriter.shouldFlush) commit(latestWriter, writerBuilder.writerFrom(latestWriter)) else A.pure(None)
    } yield ()

  /** Extracts the current offset for a given topic partition. If a topic partition does not have a committed offset, it
    * won't be returned to avoid Connect committing the offset
    * @param currentOffsets
    *   \- A sequence of topic-partition tuples and their offset information
    * @return
    */
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): F[Map[TopicPartition, OffsetAndMetadata]] =
    for {
      writers <- writersRef.get
    } yield currentOffsets.keys.flatMap { tp =>
      writers.get(tp).flatMap { writer =>
        writer.state.committedOffset.map { offset =>
          val initialOffsetAndMeta = currentOffsets(tp)
          val newOffsetAndMeta =
            new OffsetAndMetadata(offset.value, initialOffsetAndMeta.leaderEpoch(), initialOffsetAndMeta.metadata())
          tp -> newOffsetAndMeta
        }
      }
    }.toMap
}

object WriterManager extends LazyLogging {
  def from[F[_]](
    config:   EmsSinkConfig,
    sinkName: String,
    uploader: Uploader[F],
    writers:  Ref[F, Map[TopicPartition, Writer]],
  )(
    implicit
    A: Async[F]): WriterManager[F] =
    new WriterManager(
      sinkName,
      uploader,
      config.workingDir,
      new WriterBuilderImpl(config.workingDir, sinkName, config.commitPolicy, config.parquet, config.explode),
      writers,
      config.parquet.cleanup,
    )
}
