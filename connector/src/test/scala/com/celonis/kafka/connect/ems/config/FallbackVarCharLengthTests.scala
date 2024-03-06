/*
 * Copyright 2024 Celonis SE
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
