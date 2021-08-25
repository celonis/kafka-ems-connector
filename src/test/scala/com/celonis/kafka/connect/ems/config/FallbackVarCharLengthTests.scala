/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FallbackVarCharLengthTests extends AnyFunSuite with Matchers {
  test(s"return default if the key is missing") {
    EmsSinkConfig.extractFallbackVarcharLength(Map.empty) shouldBe Right(None)
  }

  test(s"return the given value") {
    val expected = 11111
    EmsSinkConfig.extractFallbackVarcharLength(Map(FALLBACK_VARCHAR_LENGTH_KEY -> expected)) shouldBe Right(
      Some(expected),
    )
  }

  test(s"return an error if the value is smaller than 1") {
    val message =
      s"Invalid [$FALLBACK_VARCHAR_LENGTH_KEY]. $FALLBACK_VARCHAR_LENGTH_DOC"
    EmsSinkConfig.extractFallbackVarcharLength(Map(FALLBACK_VARCHAR_LENGTH_KEY -> 0)) shouldBe Left(message)

    EmsSinkConfig.extractFallbackVarcharLength(Map(FALLBACK_VARCHAR_LENGTH_KEY -> -2)) shouldBe Left(message)
  }
}
