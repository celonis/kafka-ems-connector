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
