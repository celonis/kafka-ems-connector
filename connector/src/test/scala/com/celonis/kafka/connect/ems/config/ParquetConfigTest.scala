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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import org.scalatest.funsuite.AnyFunSuiteLike

class ParquetConfigTest extends AnyFunSuiteLike {
  test(s"return defaults if retry keys are missing") {
    assertResult(Right(PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT)) {
      ParquetConfig.extractParquetRowGroupSize(Map.empty, 10_000L)
    }
  }

  test(s"return the given value") {
    val rowGroupSize = 5000
    assertResult(Right(rowGroupSize)) {
      ParquetConfig.extractParquetRowGroupSize(Map(PARQUET_ROW_GROUP_SIZE_BYTES_KEY -> rowGroupSize.toString), 10_000)
    }
  }

  test(s"return an error if the value is smaller than 1") {
    val message =
      s"Invalid [$PARQUET_ROW_GROUP_SIZE_BYTES_KEY]. The parquet row group size must be at least 1."

    Seq(0, -10).foreach { value =>
      assertResult(Left(message)) {
        ParquetConfig.extractParquetRowGroupSize(Map(PARQUET_ROW_GROUP_SIZE_BYTES_KEY -> value), 10_000L)
      }
    }
  }
  test("always set cleanup strategy to delete when inmemfs is set") {
    Seq(true, false).foreach { value =>
      assertResult(Right(ParquetConfig.default))(
        ParquetConfig.extract(
          Map(DEBUG_KEEP_TMP_FILES_KEY -> value.toString),
          useInMemFs           = true,
          commitPolicyFileSize = 10_000,
        ),
      )
    }

  }
}
