/*
 * Copyright 2022 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  def extract(props: Map[String, _], useInMemFs: Boolean): Either[String, ParquetConfig] =
    ParquetConfig.extractParquetFlushRecords(props).map { rowGroupSize =>
      val keepParquetFiles = booleanOr(props, DEBUG_KEEP_TMP_FILES_KEY, DEBUG_KEEP_TMP_FILES_DOC)
        .getOrElse(DEBUG_KEEP_TMP_FILES_DEFAULT)
      ParquetConfig(
        rowGroupSize = rowGroupSize,
        cleanup      = if (!useInMemFs && keepParquetFiles) ParquetFileCleanupRename.Default else ParquetFileCleanupDelete,
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
