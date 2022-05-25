/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AuthorizationTests extends AnyFunSuite with Matchers {
  test(s"return an error if $AUTHORIZATION_KEY is missing") {
    val expectedMessage =
      s"Invalid [$AUTHORIZATION_KEY]. $AUTHORIZATION_DOC"
    AuthorizationHeader.extract(Map.empty) shouldBe Left(expectedMessage)
    AuthorizationHeader.extract(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    AuthorizationHeader.extract(Map("a" -> "b", AUTHORIZATION_KEY + ".ext" -> 1)) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $AUTHORIZATION_KEY is empty") {
    val expectedMessage =
      s"Invalid [$AUTHORIZATION_KEY]. $AUTHORIZATION_DOC"
    AuthorizationHeader.extract(Map(AUTHORIZATION_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $AUTHORIZATION_KEY is not a string") {
    val expectedMessage =
      s"Invalid [$AUTHORIZATION_KEY]. $AUTHORIZATION_DOC"

    AuthorizationHeader.extract(Map(AUTHORIZATION_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $AUTHORIZATION_KEY is not matching the expected format") {
    val expectedMessage =
      s"Invalid [$AUTHORIZATION_KEY]. $AUTHORIZATION_DOC"

    AuthorizationHeader.extract(Map(AUTHORIZATION_KEY -> "abc")) shouldBe Left(expectedMessage)
  }

  test(s"return the value provided by $AUTHORIZATION_KEY setting") {
    val expected1 = "Bearer id"
    AuthorizationHeader.extract(Map(AUTHORIZATION_KEY -> expected1)) shouldBe Right(AuthorizationHeader(expected1))

    val expected2 = "AppKey id"
    AuthorizationHeader.extract(Map(AUTHORIZATION_KEY -> expected2)) shouldBe Right(AuthorizationHeader(expected2))
  }
}
