/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.IO
import cats.effect.Ref
import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.model.TopicPartitionOffset
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import org.apache.kafka.clients.consumer.OffsetAndMetadata

import java.nio.file.Path

/**
  * Manages the lifecycle of [[Writer]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  */
class WriterManager(
  sinkName:      String,
  uploader:      Uploader,
  workingDir:    Path,
  writerBuilder: WriterBuilder,
  writersRef:    Ref[IO, Map[TopicPartition, Writer]],
) extends StrictLogging {

  /**
    * Uploads the data to EMS if the commit policy is met.
    * @return
    */
  def maybeUploadData(): IO[Unit] = {
    // The data is uploaded sequentially. We might want to parallelize the process
    logger.debug(s"[{}] Received call to WriterManager.maybeUploadData", sinkName)
    for {
      writers <- writersRef.get.map(_.values.filter(_.shouldFlush).toList)
      _       <- writers.traverse(w => commit(w))
    } yield ()
  }

  private case class CommitWriterResult(newWriter: Writer, offset: TopicPartitionOffset)

  /**
    *  Committing a file created by the writer, will result creating a new writer
    * @param writer - An instance of [[Writer]]
    * @return
    */
  private def commit(writer: Writer): IO[Option[CommitWriterResult]] = {
    val state = writer.state
    //check if data was written to the file
    if (state.records == 0) IO.pure(None)
    else {
      for {
        _   <- IO(writer.close())
        file = writer.state.file
        _ <- IO(
          logger.info(s"Uploading file: $file for topic-partition:${TopicPartition.show.show(state.topicPartition)}"),
        )
        response <- uploader.upload(file)
        _        <- IO(logger.info(s"Received ${response.asJson.noSpaces} for uploading file:$file"))
        _        <- IO(file.delete())
        newWriter = writerBuilder.writerFrom(writer)
        _        <- writersRef.update(map => map + (writer.state.topicPartition -> newWriter))
      } yield CommitWriterResult(
        newWriter,
        TopicPartitionOffset(writer.state.topicPartition.topic,
                             writer.state.topicPartition.partition,
                             writer.state.offset,
        ),
      ).some
    }
  }

  /*  private def commit(topicPartition: TopicPartition): IO[Option[TopicPartitionOffset]] = {
    writersRef.get.map(_.get(topicPartition)).flatMap(_.fold(IO.pure(None))(commit))
  }*/

  /**
    * When a partition is opened we cleanup the folder where the temp files are accumulating
    * @param partitions - A set of topic-partition tuples which the current task will own
    */
  def open(partitions: Set[TopicPartition]): IO[Unit] =
    for {
      writers <- writersRef.get.map(_.values.toList)
      _       <- writers.traverse(w => IO(w.close()))
      _       <- writersRef.update(_ => Map.empty)
      _       <- partitions.toList.traverse(partition => IO(FileSystem.cleanup(workingDir, sinkName, partition)))
    } yield ()

  def close(partitions: List[TopicPartition]): IO[Unit] =
    for {
      _ <- IO(logger.info(s"[{}] Received call to WriterManager.close(partitions):",
                          sinkName,
                          partitions.map(_.show).mkString(";"),
      ))
      writersMap   <- writersRef.get
      _            <- partitions.flatMap(writersMap.get).traverse(w => IO(w.close()))
      newWritersMap = writersMap -- partitions.toSet
      //do not cleanup the folders. on open partitions we do that.
      _ <- writersRef.update(_ => newWritersMap)
    } yield ()

  def close(): IO[Unit] =
    for {
      _          <- IO(logger.info(s"[{}] Received call to WriterManager.close()", sinkName))
      writers    <- writersRef.get.map(_.values.toList)
      partitions <- writers.traverse(w => IO(w.close()).map(_ => w.state.topicPartition))
      _          <- partitions.traverse(partition => IO(FileSystem.cleanup(workingDir, sinkName, partition)))
      _          <- writersRef.update(_ => Map.empty)
    } yield ()

  def write(record: Record): IO[Unit] =
    for {
      _ <- IO {
        logger.debug(
          s"[$sinkName] Received call to WriterManager.write for ${RecordMetadata.show.show(record.metadata)}",
        )
      }
      writersMap <- writersRef.get
      writer <- writersMap.get(record.metadata.topicPartition) match {
        case Some(value) => IO(value)
        case None =>
          IO(writerBuilder.writerFrom(record)).flatMap { writer =>
            writersRef.update(map => map + (record.metadata.topicPartition -> writer))
              .map(_ => writer)
          }
      }
      schema = record.value.schema()
      latestWriter <- {
        if (writer.shouldRollover(schema)) commit(writer).map(_.fold(writer)(_.newWriter))
        else IO(writer)
      }
      _ <- IO(latestWriter.write(record))
      _ <- if (latestWriter.shouldFlush) commit(latestWriter) else IO(None)
    } yield ()

  /**
    * Extracts the current offset for a given topic partition.
    *  If a topic partition does not have a committed offset, it won't be returned to avoid Connect committing the offset
    * @param currentOffsets - A sequence of topic-partition tuples and their offset information
    * @return
    */
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): IO[Map[TopicPartition, OffsetAndMetadata]] =
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
  def from(
    config:   EmsSinkConfig,
    sinkName: String,
    uploader: Uploader,
    writers:  Ref[IO, Map[TopicPartition, Writer]],
  ): WriterManager =
    new WriterManager(
      sinkName,
      uploader,
      config.workingDir,
      new WriterBuilderImpl(config.workingDir, sinkName, config.commitPolicy),
      writers,
    )
}
