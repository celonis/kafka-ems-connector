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
import cats.implicits._
import com.celonis.kafka.connect.ems.model.CommitContext
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.storage.formats.FormatWriter
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema

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
      logger.info(
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
