/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParquetFlushTests extends AnyFunSuite with Matchers {
  test(s"return defaults if retry keys are missing") {
    EmsSinkConfig.extractParquetFlushRecords(Map.empty) shouldBe Right(PARQUET_FLUSH_DEFAULT)
  }

  test(s"return the given value") {
    val expected = 11111
    EmsSinkConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> expected)) shouldBe Right(expected)
    EmsSinkConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> expected)) shouldBe Right(expected)
  }

  test(s"return an error if the value is smaller than 1") {
    val message =
      s"Invalid [$PARQUET_FLUSH_KEY]. The number of records to flush the parquet file needs to be greater or equal to 1."
    EmsSinkConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> 0)) shouldBe Left(message)

    EmsSinkConfig.extractParquetFlushRecords(Map(PARQUET_FLUSH_KEY -> -2)) shouldBe Left(message)
  }
}
