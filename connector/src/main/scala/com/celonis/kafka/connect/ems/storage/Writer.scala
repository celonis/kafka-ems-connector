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
