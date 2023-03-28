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
import com.celonis.kafka.connect.ems.config.ExplodeConfig
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.storage.formats.ParquetFormatWriter

import java.nio.file.Path

trait WriterBuilder {

  /** Creates a new writer from an existing one. This happens only when the data(i.e. file) is committed
    * @param writer
    *   \- An instance of [[Writer]]
    * @return
    */
  def writerFrom(writer: Writer): Writer

  /** Creates a new [[Writer]] whenever the first record arrives for a topic-partition.
    * @param record
    *   \- An instance of [[Record]]
    * @return
    */
  def writerFrom(record: Record): Writer
}

class WriterBuilderImpl(
  tempDir:      Path,
  sinkName:     String,
  commitPolicy: CommitPolicy,
  parquet:      ParquetConfig,
  explode:      ExplodeConfig,
  fileSystem:   FileSystemOperations,
) extends WriterBuilder {

  /** Creates a new writer from an existing one. This happens only when the data(i.e. file) is committed
    * @param writer
    *   \- An instance of [[Writer]]
    * @return
    */
  def writerFrom(writer: Writer): Writer = {
    val currentState   = writer.state
    val topicPartition = currentState.topicPartition
    val output         = fileSystem.createOutput(tempDir, sinkName, topicPartition)
    val newState = currentState.copy(
      committedOffset = Some(currentState.offset),
      fileSize        = 0.toLong,
      records         = 0.toLong,
      lastWriteTs     = System.currentTimeMillis(),
      file            = output.outputFile(),
    )

    val formatWriter =
      ParquetFormatWriter.from(output, explode.explodeSchema(currentState.schema), parquet, explode.toExplodeFn)
    new EmsWriter(sinkName, commitPolicy, formatWriter, newState)
  }

  /** Creates a new [[Writer]] whenever the first record arrives for a topic-partition.
    * @param record
    *   \- An instance of [[Record]]
    * @return
    */
  def writerFrom(record: Record): Writer = {
    val output = fileSystem.createOutput(tempDir, sinkName, record.metadata.topicPartition)
    val formatWriter =
      ParquetFormatWriter.from(output, explode.explodeSchema(record.value.getSchema), parquet, explode.toExplodeFn)
    val state = WriterState(
      record.metadata.topicPartition,
      // creates the state from the record. the data hasn't been yet written
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
