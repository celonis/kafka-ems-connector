/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import org.apache.avro.generic.GenericRecord

trait FormatWriter extends AutoCloseable {
  def size: Long

  def rolloverFileOnSchemaChange(): Boolean

  def write(value: GenericRecord): Unit
}
