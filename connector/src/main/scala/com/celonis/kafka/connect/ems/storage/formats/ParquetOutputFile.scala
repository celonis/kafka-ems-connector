/*
 * Copyright 2017-2022 Celonis Ltd
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
    override def getPos: Long = fs.size
    override def write(b: Int): Unit = fs.write(b)
    override def write(b: Array[Byte]): Unit = fs.write(b)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = fs.write(b, off, len)
    override def flush(): Unit = fs.flush()
  }
}
