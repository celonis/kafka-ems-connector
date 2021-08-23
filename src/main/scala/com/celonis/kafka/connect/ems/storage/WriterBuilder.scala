/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.storage.formats.ParquetFormatWriter

import java.nio.file.Path

trait WriterBuilder {

  /**
    * Creates a new writer from an existing one. This happens only when the data(i.e. file) is committed
    * @param writer - An instance of [[Writer]]
    * @return
    */
  def writerFrom(writer: Writer): Writer

  /**
    * Creates a new [[Writer]] whenever the first record arrives for a topic-partition.
    * @param record - An instance of [[Record]]
    * @return
    */
  def writerFrom(record: Record): Writer
}

class WriterBuilderImpl(tempDir: Path, sinkName: String, commitPolicy: CommitPolicy, parquet: ParquetConfig)
    extends WriterBuilder {

  /**
    * Creates a new writer from an existing one. This happens only when the data(i.e. file) is committed
    * @param writer - An instance of [[Writer]]
    * @return
    */
  def writerFrom(writer: Writer): Writer = {
    val currentState   = writer.state
    val topicPartition = currentState.topicPartition
    val output         = FileSystem.createOutput(tempDir, sinkName, topicPartition)
    val newState = currentState.copy(
      committedOffset = Some(currentState.offset),
      fileSize        = 0.toLong,
      records         = 0.toLong,
      lastWriteTs     = System.currentTimeMillis(),
      file            = output.outputFile(),
    )

    val formatWriter = ParquetFormatWriter.from(output, currentState.schema, parquet)
    new EmsWriter(sinkName, commitPolicy, formatWriter, newState)
  }

  /**
    * Creates a new [[Writer]] whenever the first record arrives for a topic-partition.
    * @param record - An instance of [[Record]]
    * @return
    */
  def writerFrom(record: Record): Writer = {
    val output       = FileSystem.createOutput(tempDir, sinkName, record.metadata.topicPartition)
    val formatWriter = ParquetFormatWriter.from(output, record.value.getSchema, parquet)
    val state = WriterState(
      record.metadata.topicPartition,
      //creates the state from the record. the data hasn't been yet written
      // The connector uses this to filter out records which were processed
      new Offset(record.metadata.offset.value - 1),
      None,
      0L,
      0L,
      System.currentTimeMillis(),
      record.value.getSchema,
      output.outputFile(),
    )
    new EmsWriter(
      sinkName,
      commitPolicy,
      formatWriter,
      state,
    )
  }

}
