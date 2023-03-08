package com.celonis.kafka.connect.ems.storage

import com.celonis.kafka.connect.ems.config.ExplodeConfig
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.formats.{FormatWriter, InMemoryParquetFormatWriter, ParquetFormatWriter}
import org.apache.avro.Schema
import org.apache.commons.io.output.ByteArrayOutputStream

import java.nio.file.Path

trait BehaviourBuilder {
  def createOutput(sinkName: String, topicPartition: TopicPartition): Output

  def createFormatWriter(schema: Schema): FormatWriter
}

class FileBuilder(tempDir: Path, parquet: ParquetConfig, explode: ExplodeConfig) extends BehaviourBuilder {
  override def createOutput(sinkName: String, topicPartition: TopicPartition): Output =
    FileSystem.createOutput(tempDir, sinkName, topicPartition)

  override def createFormatWriter(schema: Schema): FormatWriter =
    ParquetFormatWriter.from(output, explode.explodeSchema(schema), parquet, explode.toExplodeFn)
}

class InMemoryBuilder(parquet: ParquetConfig, explode: ExplodeConfig) extends BehaviourBuilder{
  override def createOutput(sinkName: String, topicPartition: TopicPartition): Output = new InMemoryFile(new ByteArrayOutputStream())

  override def createFormatWriter(schema: Schema): FormatWriter = InMemoryParquetFormatWriter.from(output, explode.explodeSchema(schema), parquet, explode.toExplodeFn)
}