/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.parquet
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

import java.io.File
import java.io.FileInputStream

class ParquetLocalInputFile(file: File) extends InputFile {

  def getLength: Long = file.length()

  def newStream(): SeekableInputStream = new ParquetSeekableStream(new FileInputStream(file))
}
