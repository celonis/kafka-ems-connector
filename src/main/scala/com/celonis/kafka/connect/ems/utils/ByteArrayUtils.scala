/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.utils
import java.nio.ByteBuffer

object ByteArrayUtils {
  def longToByteArray(l: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(l)
    val ret = buffer.array()
    require(ret.size == java.lang.Long.BYTES)
    ret
  }

}
