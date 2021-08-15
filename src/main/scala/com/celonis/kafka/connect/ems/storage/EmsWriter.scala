/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.implicits._
import com.celonis.kafka.connect.ems.model.CommitContext
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.storage.formats.FormatWriter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema

class EmsWriter(
  sinkName:     String,
  commitPolicy: CommitPolicy,
  formatWriter: FormatWriter,
  initialState: WriterState,
) extends Writer
    with LazyLogging {

  private var internalState: WriterState = initialState

  override def write(record: Record): Unit = {
    logger.debug("[{}] EmsWriter.write: Internal state: {}", sinkName, internalState.show)
    if (record.metadata.offset > internalState.offset) {
      formatWriter.write(record.value)

      internalState = internalState.copy(
        fileSize    = formatWriter.size,
        records     = internalState.records + 1,
        offset      = record.metadata.offset,
        lastWriteTs = System.currentTimeMillis(),
      )
    } else {
      logger.debug(
        "[{}] EmsWriter.write: ignoring record. Offset is already processed. current={} received={}",
        sinkName,
        internalState.offset.show,
        record.metadata.offset.show,
      )
    }
  }

  override def shouldRollover(schema: Schema): Boolean =
    formatWriter.rolloverFileOnSchemaChange() && internalState.schema != schema

  override def close(): Unit = formatWriter.close()

  override def shouldFlush: Boolean = {
    val commitContext = CommitContext(
      internalState.records,
      internalState.fileSize,
      internalState.lastWriteTs,
    )

    commitPolicy.shouldFlush(commitContext)
  }

  override def state: WriterState = internalState
}
