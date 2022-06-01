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

package com.celonis.kafka.connect.ems
import com.github.tomakehurst.wiremock.http.multipart.PartParser
import org.apache.avro.generic.GenericRecord
import org.apache.commons.fileupload.FileUpload
import org.apache.commons.fileupload.disk.DiskFileItemFactory
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.mockserver.model.HttpRequest

import java.io.File

package object parquet {

  private val fileUpload = {
    val fileItemFactory = new DiskFileItemFactory(Integer.MAX_VALUE, new File(System.getProperty("java.io.tmpdir")))
    new FileUpload(fileItemFactory)
  }

  def extractParquetFromRequest(httpRequest: HttpRequest): File = {
    val contentType = httpRequest.getHeader("Content-Type").iterator().next()

    val uploadContext = new PartParser.ByteArrayUploadContext(
      httpRequest.getBody.getRawBytes,
      null,
      contentType,
    )

    val items = fileUpload.parseRequest(uploadContext)
    val file  = File.createTempFile("temp", ".parquet")
    items.iterator().next().write(file)
    file
  }

  def parquetReader(file: File): ParquetReader[GenericRecord] = {
    val parquetLocalInputFile = new ParquetLocalInputFile(file)
    AvroParquetReader.builder[GenericRecord](parquetLocalInputFile).build
  }
}
