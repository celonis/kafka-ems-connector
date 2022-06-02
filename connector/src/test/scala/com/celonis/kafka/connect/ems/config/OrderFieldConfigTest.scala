/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_KEY
import com.celonis.kafka.connect.ems.conversion.NoOpOrderFieldInserter
import com.celonis.kafka.connect.ems.conversion.OrderFieldInserter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.syntax.option._

class OrderFieldConfigTest extends AnyFunSuite with Matchers {
  test("No order field and no inserter when no primary keys are set") {
    val configs = Map(
      ORDER_FIELD_NAME_KEY -> "should_not",
    )
    OrderFieldConfig.from(configs, Nil) shouldBe OrderFieldConfig(None, NoOpOrderFieldInserter)
  }

  test("Default order field name when primary keys") {
    val configs = Map(
      "something_else" -> "should_not",
      PRIMARY_KEYS_KEY -> "a",
    )
    OrderFieldConfig.from(configs, List("a")) shouldBe OrderFieldConfig(OrderFieldInserter.FieldName.some,
                                                                        OrderFieldInserter,
    )
  }

  test("Use the provided sortable field and No-op inserter") {
    val configs = Map(
      ORDER_FIELD_NAME_KEY -> "abc",
      PRIMARY_KEYS_KEY     -> "a",
    )
    OrderFieldConfig.from(configs, List("a")) shouldBe OrderFieldConfig("abc".some, NoOpOrderFieldInserter)
  }
}
