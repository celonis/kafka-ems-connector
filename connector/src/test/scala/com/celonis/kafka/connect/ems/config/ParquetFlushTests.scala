/*
 * Copyright 2023 Celonis SE
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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParquetFlushTests extends AnyFunSuite with Matchers {
  test(s"return defaults if retry keys are missing") {
    ParquetConfig.extractParquetFlushRecords(Map.empty) shouldBe Right(PARQUET_FLUSH_DEFAULT)
  }

  test(s"return the given value") {
    val expected = 11111
    ParquetConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> expected)) shouldBe Right(expected)
    ParquetConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> expected)) shouldBe Right(expected)
  }

  test(s"return an error if the value is smaller than 1") {
    val message =
      s"Invalid [$PARQUET_FLUSH_KEY]. The number of records to flush the parquet file needs to be greater or equal to 1."
    ParquetConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> 0)) shouldBe Left(message)

    ParquetConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> -2)) shouldBe Left(message)
  }
}
