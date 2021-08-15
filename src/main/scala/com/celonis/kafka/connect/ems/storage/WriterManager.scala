/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.implicits.toShow
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.model.TopicPartitionOffset
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
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
) extends StrictLogging {
  private val writers = scala.collection.mutable.Map.empty[TopicPartition, Writer]

  /**
    * Uploads the data to EMS if the commit policy is met.
    * @return
    */
  def maybeUploadData(): Unit = {
    // The data is uploaded sequentially. We might want to parallelize the process
    logger.debug(s"[{}] Received call to WriterManager.maybeUploadData", sinkName)
    writers.values.filter(_.shouldFlush).foreach(commit)
  }

  /**
    *  Committing a file created by the writer, will result creating a new writer
    * @param writer - An instance of [[Writer]]
    * @return
    */
  private def commit(writer: Writer): Option[TopicPartitionOffset] = {
    val state = writer.state
    //check if data was written to the file
    if (state.records == 0) None
    else {
      writer.close()
      val file = writer.state.file
      uploader.upload(file)
      file.delete()
      val newWriter = writerBuilder.writerFrom(writer)
      writers += writer.state.topicPartition -> newWriter
      val offset = TopicPartitionOffset(writer.state.topicPartition.topic,
                                        writer.state.topicPartition.partition,
                                        writer.state.offset,
      )
      Some(offset)
    }
  }

  private def commit(topicPartition: TopicPartition): Option[TopicPartitionOffset] =
    writers.get(topicPartition).flatMap(commit)

  /**
    * When a partition is opened we cleanup the folder where the temp files are accumulating
    * @param partitions - A set of topic-partition tuples which the current task will own
    */
  def open(partitions: Set[TopicPartition]): Unit = {
    writers.values.foreach(_.close())
    writers.clear()
    partitions.foreach { partition =>
      FileSystem.cleanup(workingDir, sinkName, partition)
    }
  }

  def close(): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", sinkName)
    writers.foreach {
      case (tp, writer) =>
        writer.close()
        FileSystem.cleanup(workingDir, sinkName, tp)
    }
    writers.clear()
  }

  def write(record: Record): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.write for {}", sinkName, record.metadata.show)

    val writer = writers.getOrElseUpdate(record.metadata.topicPartition, writerBuilder.writerFrom(record))
    val schema = record.value.schema()
    if (writer.shouldRollover(schema)) {
      commit(record.metadata.topicPartition)
    }
    //we might have had the commit above and as a result a new writer
    val latestWriter = writers(record.metadata.topicPartition)
    latestWriter.write(record)

    // Is accumulated data requiring an upload?!
    if (writer.shouldFlush)
      commit(record.metadata.topicPartition)
    ()
  }

  /**
    * Extracts the current offset for a given topic partition.
    *  If a topic partition does not have a committed offset, it won't be returned to avoid Connect committing the offset
    * @param currentOffsets - A sequence of topic-partition tuples and their offset information
    * @return
    */
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets.keys.flatMap { tp =>
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
  def from(config: EmsSinkConfig, sinkName: String, uploader: Uploader): WriterManager =
    new WriterManager(
      sinkName,
      uploader,
      config.workingDir,
      new WriterBuilderImpl(config.workingDir, sinkName, config.commitPolicy),
    )
}
