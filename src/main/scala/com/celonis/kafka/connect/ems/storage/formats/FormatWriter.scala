/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import org.apache.kafka.connect.data.Struct

trait FormatWriter extends AutoCloseable {
  def size: Long

  def rolloverFileOnSchemaChange(): Boolean

  def write(value: Struct): Unit
}
