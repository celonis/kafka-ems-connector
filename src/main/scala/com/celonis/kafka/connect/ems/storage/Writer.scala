/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.Show
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.Topic
import org.apache.kafka.connect.data.Schema

import java.io.File

trait Writer extends AutoCloseable {
  def shouldFlush: Boolean

  def write(record: Record): Unit

  def shouldRollover(schema: Schema): Boolean

  def state: WriterState
}

case class WriterState(
  topic:           Topic,
  partition:       Partition,
  offset:          Offset,
  committedOffset: Option[Offset],
  fileSize:        Long,
  records:         Long,
  lastWriteTs:     Long,
  schema:          Schema,
  file:            File,
)

object WriterState {
  implicit val show: Show[WriterState] = Show.show { state =>
    s"${state.topic.value}-${state.partition.value}:${state.offset.value} ${state.committedOffset.map(
      _.value.toString,
    ).getOrElse("-")} ${state.records} ${state.fileSize} ${state.lastWriteTs}"
  }
}
