/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.conversion.ToAvroDataConverter
import com.celonis.kafka.connect.ems.storage.FileAndStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ParquetFormatWriter(output: FileAndStream, writer: ParquetWriter[AnyRef]) extends FormatWriter with LazyLogging {
  override def write(value: Struct): Unit = {
    val genericRecord: AnyRef = ToAvroDataConverter.convertToGenericRecord(value)
    writer.write(genericRecord)
  }

  override def rolloverFileOnSchemaChange() = true

  override def close(): Unit = {
    writer.close()
    output.close()
  }

  override def size: Long = output.size
}

object ParquetFormatWriter {
  def from(output: FileAndStream, connectSchema: ConnectSchema, config: ParquetConfig): ParquetFormatWriter = {
    val schema: Schema = ToAvroDataConverter.convertSchema(connectSchema)
    val outputFile = new ParquetOutputFile(output)

    val writer: ParquetWriter[AnyRef] = AvroParquetWriter
      .builder[AnyRef](outputFile)
      .withPageSize(DEFAULT_PAGE_SIZE)
      .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withRowGroupSize(config.rowGroupSize)
      .withSchema(schema)
      .build()

    new ParquetFormatWriter(output, writer)
  }
}
