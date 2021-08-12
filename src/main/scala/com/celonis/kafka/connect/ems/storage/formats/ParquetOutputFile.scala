/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import com.celonis.kafka.connect.ems.storage.FileAndStream
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream

class ParquetOutputFile(fileAndStream: FileAndStream) extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream =
    new PositionOutputStreamWrapper(fileAndStream)

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    new PositionOutputStreamWrapper(fileAndStream)

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long = 0

  class PositionOutputStreamWrapper(fs: FileAndStream) extends PositionOutputStream {
    override def getPos: Long = fs.file.length()
    override def write(b: Int): Unit = fs.stream.write(b)
  }
}
