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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TargetTableTests extends AnyFunSuite with Matchers {
  test(s"return an error if $TARGET_TABLE_KEY is missing") {
    val expectedMessage =
      s"Invalid [$TARGET_TABLE_KEY]. The table in EMS to store the data."
    EmsSinkConfig.extractTargetTable(Map.empty) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractTargetTable(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractTargetTable(Map("a" -> "b", TARGET_TABLE_KEY + ".ext" -> 1)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $TARGET_TABLE_KEY is empty") {
    val expectedMessage =
      s"Invalid [$TARGET_TABLE_KEY]. The table in EMS to store the data."
    EmsSinkConfig.extractTargetTable(Map(TARGET_TABLE_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $TARGET_TABLE_KEY is null") {
    val expectedMessage =
      s"Invalid [$TARGET_TABLE_KEY]. The table in EMS to store the data."
    EmsSinkConfig.extractTargetTable(Map(TARGET_TABLE_KEY -> null)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $TARGET_TABLE_KEY is not a string") {
    val expectedMessage =
      s"Invalid [$TARGET_TABLE_KEY]. The table in EMS to store the data."
    EmsSinkConfig.extractTargetTable(Map(TARGET_TABLE_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return the target table provided by $TARGET_TABLE_KEY") {
    val expected = "picaboo"
    EmsSinkConfig.extractTargetTable(Map(TARGET_TABLE_KEY -> expected)) shouldBe Right(expected)
  }
}
