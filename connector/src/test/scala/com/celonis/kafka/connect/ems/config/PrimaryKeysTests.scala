/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PrimaryKeysTests extends AnyFunSuite with Matchers {
  test(s"return NIL if $PRIMARY_KEYS_KEY is missing") {
    EmsSinkConfig.extractPrimaryKeys(Map.empty) shouldBe Right(Nil)
    EmsSinkConfig.extractPrimaryKeys(Map("a" -> "b", "b" -> 1)) shouldBe Right(Nil)
    EmsSinkConfig.extractPrimaryKeys(Map("a" -> "b", ENDPOINT_KEY + ".ext" -> 1)) shouldBe Right(Nil)
  }

  test(s"return an error if $PRIMARY_KEYS_KEY contains invalid field names") {
    EmsSinkConfig.extractPrimaryKeys(Map(PRIMARY_KEYS_KEY -> "a-c")) shouldBe Left(
      s"Invalid [$PRIMARY_KEYS_KEY]. Illegal character found for: a-c. $PRIMARY_KEYS_DOC",
    )
    EmsSinkConfig.extractPrimaryKeys(Map(PRIMARY_KEYS_KEY -> "12abc")) shouldBe Left(
      s"Invalid [$PRIMARY_KEYS_KEY]. Illegal character found for: 12abc. $PRIMARY_KEYS_DOC",
    )
  }

  test("return the primary keys") {
    EmsSinkConfig.extractPrimaryKeys(
      Map(PRIMARY_KEYS_KEY -> "abc, foo, boo "),
    ) shouldBe Right(List("abc", "foo", "boo"))
  }
}
