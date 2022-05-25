/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config
import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config.PropertiesHelper._
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanup
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupDelete
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupRename

case class ParquetConfig(rowGroupSize: Int, cleanup: ParquetFileCleanup)

object ParquetConfig {
  val Default: ParquetConfig = ParquetConfig(1000, ParquetFileCleanupDelete)

  def extract(props: Map[String, _]): Either[String, ParquetConfig] =
    ParquetConfig.extractParquetFlushRecords(props).map { rowGroupSize =>
      val keepParquetFiles = booleanOr(props, DEBUG_KEEP_TMP_FILES_KEY, DEBUG_KEEP_TMP_FILES_DOC)
        .getOrElse(DEBUG_KEEP_TMP_FILES_DEFAULT)
      ParquetConfig(
        rowGroupSize = rowGroupSize,
        cleanup      = if (keepParquetFiles) ParquetFileCleanupRename else ParquetFileCleanupDelete,
      )
    }

  def extractParquetFlushRecords(props: Map[String, _]): Either[String, Int] =
    PropertiesHelper.getInt(props, PARQUET_FLUSH_KEY) match {
      case Some(value) =>
        if (value < 1)
          error(PARQUET_FLUSH_KEY, "The number of records to flush the parquet file needs to be greater or equal to 1.")
        else value.asRight[String]
      case None => PARQUET_FLUSH_DEFAULT.asRight
    }
}
