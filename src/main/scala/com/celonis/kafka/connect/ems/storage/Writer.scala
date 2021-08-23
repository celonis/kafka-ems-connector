/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.Show
import cats.implicits.toShow
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.TopicPartition
import org.apache.avro.Schema

import java.io.File

trait Writer extends AutoCloseable {
  def shouldFlush: Boolean

  def write(record: Record): Unit

  def shouldRollover(schema: Schema): Boolean

  def state: WriterState
}

case class WriterState(
  topicPartition:  TopicPartition,
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
    s"${state.topicPartition.topic.show}-${state.topicPartition.partition.show}:${state.offset.show} ${state.committedOffset.map(
      _.value.toString,
    ).getOrElse("-")} records=${state.records} fileSize=${state.fileSize} lastWrite=${state.lastWriteTs}"
  }
}
