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
