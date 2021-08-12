/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import com.celonis.kafka.connect.ems.model.SinkData

trait FormatWriter extends AutoCloseable {
  def size: Long

  def rolloverFileOnSchemaChange(): Boolean

  def write(value: SinkData): Unit
}
