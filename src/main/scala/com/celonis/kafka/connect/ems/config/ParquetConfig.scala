/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanup
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupDelete

case class ParquetConfig(rowGroupSize: Int, cleanup: ParquetFileCleanup)

object ParquetConfig {
  val Default: ParquetConfig = ParquetConfig(1000, ParquetFileCleanupDelete)
}
