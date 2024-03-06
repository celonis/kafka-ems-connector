/*
 * Copyright 2024 Celonis SE
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

import java.nio.file.Path

trait Writer extends AutoCloseable {
  def shouldFlush: Boolean

  def write(record: Record): Unit

  def shouldRollover(schema: Schema): Boolean

  def state: WriterState
}

final case class WriterState(
  topicPartition:  TopicPartition,
  firstOffset:     Option[Offset],
  lastOffset:      Offset,
  committedOffset: Option[Offset],
  fileSize:        Long,
  records:         Long,
  lastWriteTs:     Long,
  schema:          Schema,
  file:            Path,
)

object WriterState {
  implicit val show: Show[WriterState] = Show.show { state =>
    String.format(
      "%s-%s: firstOffset:%s lastOffset:%s committedOffset:%s records=%d fileSize=%d lastWrite=%d",
      state.topicPartition.topic.show,
      state.topicPartition.partition.show,
      state.firstOffset.map(_.show).getOrElse("NA"),
      state.lastOffset.show,
      state.committedOffset.map(_.value.toString).getOrElse("NA"),
      state.records,
      state.fileSize,
      state.lastWriteTs,
    )
  }
}
