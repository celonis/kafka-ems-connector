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

package com.celonis.kafka.connect.ems.storage.formats

import cats.data.NonEmptySeq
import com.celonis.kafka.connect.ems.config.ParquetConfig
import com.celonis.kafka.connect.ems.storage.FileAndStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class ParquetFormatWriter(
  output:    FileAndStream,
  writer:    ParquetWriter[AnyRef],
  explodeFn: GenericRecord => NonEmptySeq[GenericRecord],
) extends FormatWriter
    with LazyLogging {
  override def write(value: GenericRecord): Unit = { val _ = explodeFn(value).map(writer.write) }

  override def rolloverFileOnSchemaChange() = true

  override def close(): Unit = {
    writer.close()
    output.close()
  }

  override def size: Long = output.size
}

object ParquetFormatWriter {
  def from(
    output:    FileAndStream,
    schema:    Schema,
    config:    ParquetConfig,
    explodeFn: GenericRecord => NonEmptySeq[GenericRecord]): ParquetFormatWriter = {
    val outputFile = new ParquetOutputFile(output)

    val writer: ParquetWriter[AnyRef] = AvroParquetWriter
      .builder[AnyRef](outputFile)
      // .withPageSize(DEFAULT_PAGE_SIZE)
      // .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withRowGroupSize(config.rowGroupSize.toLong)
      .withSchema(schema)
      .build()

    new ParquetFormatWriter(output, writer, explodeFn)
  }
}
