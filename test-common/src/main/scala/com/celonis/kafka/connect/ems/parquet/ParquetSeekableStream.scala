/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.parquet
import org.apache.parquet.io.DelegatingSeekableInputStream

import java.io.FileInputStream

class ParquetSeekableStream(fileInputStream: FileInputStream) extends DelegatingSeekableInputStream(fileInputStream) {

  var pos: Long = 0

  def getPos: Long = pos

  def seek(newPos: Long): Unit = {
    fileInputStream.getChannel.position(newPos)
    pos = newPos
  }
}
