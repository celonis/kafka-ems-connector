/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.storage.FileAndStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ParquetFormatWriter(output: FileAndStream, writer: ParquetWriter[AnyRef]) extends FormatWriter with LazyLogging {
  override def write(value: GenericRecord): Unit =
    writer.write(value)

  override def rolloverFileOnSchemaChange() = true

  override def close(): Unit = {
    writer.close()
    output.close()
  }

  override def size: Long = output.size
}

object ParquetFormatWriter {
  def from(output: FileAndStream, schema: Schema, config: ParquetConfig): ParquetFormatWriter = {
    val outputFile = new ParquetOutputFile(output)

    val writer: ParquetWriter[AnyRef] = AvroParquetWriter
      .builder[AnyRef](outputFile)
      .withPageSize(DEFAULT_PAGE_SIZE)
      //.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withRowGroupSize(config.rowGroupSize)
      .withSchema(schema)
      .build()

    new ParquetFormatWriter(output, writer)
  }
}
