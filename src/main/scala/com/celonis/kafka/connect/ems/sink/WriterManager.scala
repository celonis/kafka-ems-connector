/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.model.TopicPartitionOffset
import com.celonis.kafka.connect.ems.storage.formats.ParquetFormatWriter
import com.celonis.kafka.connect.ems.storage.EmsWriter
import com.celonis.kafka.connect.ems.storage.FileSystem
import com.celonis.kafka.connect.ems.storage.Uploader
import com.celonis.kafka.connect.ems.storage.Writer
import com.celonis.kafka.connect.ems.storage.WriterState
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
  sinkName:     String,
  uploader:     Uploader,
  commitPolicy: CommitPolicy,
  tempDir:      Path,
) extends StrictLogging {
  private val writers = scala.collection.mutable.Map.empty[TopicPartition, Writer]

  /**
    * Uploads the data to EMS if the commit policy is met.
    * @return
    */
  def maybeUploadData(): Unit = {
    val shouldFlush = writers.values.exists(_.shouldFlush)
    if (shouldFlush) {
      // The data is uploaded sequentially. We might want to parallelise the process
      logger.debug(s"[{}] Received call to WriterManager.commit", sinkName)
      writers.foreach {
        case (tp, writer) =>
          commit(tp, writer).map(offset => tp -> offset.offset)
      }
    }
  }

  private def commit(topicPartition: TopicPartition, writer: Writer): Option[TopicPartitionOffset] = {
    val state = writer.state
    //check if data was written to the file
    if (state.records == 0) None
    else {
      val file = writer.state.file
      uploader.upload(file)
      file.delete()
      val newWriter = writerFrom(writer)
      writers += topicPartition -> newWriter
      val offset = TopicPartitionOffset(topicPartition.topic, topicPartition.partition, writer.state.offset)
      Some(offset)
    }
  }

  private def commit(topicPartition: TopicPartition): Option[TopicPartitionOffset] =
    writers.get(topicPartition).flatMap(commit(topicPartition, _))

  /**
    * When a partition is opened we cleanup the folder where the temp files are accumulating
    * @param partitions - A set of topic-partition tuples which the current task will own
    */
  def open(partitions: Set[TopicPartition]): Unit = {
    writers.values.foreach(_.close())
    writers.clear()
    partitions.foreach { partition =>
      FileSystem.cleanup(tempDir, sinkName, partition)
    }
  }

  def close(): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", sinkName)
    writers.values.foreach(_.close())
    writers.clear()
  }

  def write(record: Record): Unit = {
    logger.debug(
      s"[{}] Received call to WriterManager.write for {}-{}:{}",
      sinkName,
      record.metadata.topicPartition.topic.value,
      record.metadata.topicPartition.partition.value,
      record.metadata.offset.value,
    )

    val writer = writers.getOrElseUpdate(record.metadata.topicPartition, writerFrom(record))
    val schema = record.value.schema()
    if (writer.shouldRollover(schema)) {
      commit(record.metadata.topicPartition)
    }
    //we might have had the commit above
    val latestWriter = writers(record.metadata.topicPartition)
    latestWriter.write(record)

    // Is accumulated data requiring an upload?!
    if (writer.shouldFlush)
      commit(record.metadata.topicPartition)
    ()
  }

  /**
    * Creates a new writer from an existing one. This happens only when the data(i.e. file) is committed
    * @param writer - An instance of [[Writer]]
    * @return
    */
  private def writerFrom(writer: Writer): Writer = {
    val currentState   = writer.state
    val topicPartition = TopicPartition(currentState.topic, currentState.partition)
    val output         = FileSystem.createOutput(tempDir, sinkName, topicPartition)
    val newState = currentState.copy(
      committedOffset = Some(currentState.offset),
      fileSize        = 0.toLong,
      records         = 0.toLong,
      lastWriteTs     = System.currentTimeMillis(),
      file            = output.file,
    )

    val formatWriter = ParquetFormatWriter.from(output, currentState.schema)
    new EmsWriter(sinkName, commitPolicy, formatWriter, newState)
  }

  /**
    * Creates a new [[Writer]] whenever the first record arrives for a topic-partition.
    * @param record - An instance of [[Record]]
    * @return
    */
  private def writerFrom(record: Record): Writer = {
    val output       = FileSystem.createOutput(tempDir, sinkName, record.metadata.topicPartition)
    val formatWriter = ParquetFormatWriter.from(output, record.value.schema())
    val state = WriterState(
      record.metadata.topicPartition.topic,
      record.metadata.topicPartition.partition,
      //creates the state from the record. the data hasn't been yet written
      // The connector uses this to filter out records which were processed
      new Offset(record.metadata.offset.value - 1),
      None,
      0L,
      0L,
      System.currentTimeMillis(),
      record.value.schema(),
      output.file,
    )
    new EmsWriter(
      sinkName,
      commitPolicy,
      formatWriter,
      state,
    )
  }

  /**
    * Extracts the current offset for a given topic partition, or the one provided by Connect
    * @param currentOffsets - A sequence of topic-partition tuples and their offset information
    * @return
    */
  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets.map {
      case (tp, offset) =>
        val value = writers.get(tp).map { writer =>
          writer.state.committedOffset.map { o =>
            new OffsetAndMetadata(o.value, offset.leaderEpoch(), offset.metadata())
          }.getOrElse(offset)
        }.getOrElse(offset)
        tp -> value
    }
}

object WriterManager extends LazyLogging {
  def from(config: EmsSinkConfig, sinkName: String, uploader: Uploader): WriterManager =
    new WriterManager(
      sinkName,
      uploader,
      config.commitPolicy,
      config.tempDirectory,
    )
}
