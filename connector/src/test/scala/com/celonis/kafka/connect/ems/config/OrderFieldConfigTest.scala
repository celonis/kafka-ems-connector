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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.syntax.option._
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadataFieldInserter

class OrderFieldConfigTest extends AnyFunSuite with Matchers {
  test("No order field and no inserter when no primary keys are set") {
    val configs = Map(
      ORDER_FIELD_NAME_KEY -> "should_not",
    )
    OrderFieldConfig.from(configs, Nil) shouldBe OrderFieldConfig(None)
  }

  test("Default order field name when primary keys") {
    val configs = Map(
      "something_else" -> "should_not",
      PRIMARY_KEYS_KEY -> "a",
    )
    OrderFieldConfig.from(configs, List("a")) shouldBe OrderFieldConfig(
      EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName.some,
    )
  }

  test("Use the provided sortable field and No-op inserter") {
    val configs = Map(
      ORDER_FIELD_NAME_KEY -> "abc",
      PRIMARY_KEYS_KEY     -> "a",
    )
    OrderFieldConfig.from(configs, List("a")) shouldBe OrderFieldConfig("abc".some)
  }
}
