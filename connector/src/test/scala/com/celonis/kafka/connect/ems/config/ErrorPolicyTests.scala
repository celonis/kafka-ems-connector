/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Continue
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Throw
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ErrorPolicyTests extends AnyFunSuite with Matchers {
  test(s"return an error if $ERROR_POLICY_KEY is missing") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    EmsSinkConfig.extractErrorPolicy(Map.empty) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractErrorPolicy(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractErrorPolicy(Map("a" -> "b", ERROR_POLICY_KEY + ".ext" -> 1)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is empty") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is not a string") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is invalid") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "retry.")) shouldBe Left(expectedMessage)
  }

  test(s"return the target table provided with $ERROR_POLICY_KEY") {
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "retry")) shouldBe Right(Retry)
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "throw")) shouldBe Right(Throw)
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "continue")) shouldBe Right(Continue)

    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "rEtry")) shouldBe Right(Retry)
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "THROW")) shouldBe Right(Throw)
    EmsSinkConfig.extractErrorPolicy(Map(ERROR_POLICY_KEY -> "conTinue")) shouldBe Right(Continue)
  }
}
