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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_CONTINUE_ON_INVALID_INPUT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Continue
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.ContinueOnInvalidInput
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Throw
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ErrorPolicyConfigTests extends AnyFunSuite with Matchers {
  test(s"extract returns an error if $ERROR_POLICY_KEY is missing") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicyConfig.extract(Map.empty) shouldBe Left(expectedMessage)
    ErrorPolicyConfig.extract(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    ErrorPolicyConfig.extract(Map("a" -> "b", ERROR_POLICY_KEY + ".ext" -> 1)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is empty") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is not a string") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is invalid") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "retry.")) shouldBe Left(expectedMessage)
  }

  test(s"return the target policy type provided with $ERROR_POLICY_KEY") {
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "retry")).map(_.policyType) shouldBe Right(ErrorPolicyType.RETRY)
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "throw")).map(_.policyType) shouldBe Right(ErrorPolicyType.THROW)
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "continue")).map(_.policyType) shouldBe Right(
      ErrorPolicyType.CONTINUE,
    )

    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "rEtry")).map(_.policyType) shouldBe Right(ErrorPolicyType.RETRY)
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "THROW")).map(_.policyType) shouldBe Right(ErrorPolicyType.THROW)
    ErrorPolicyConfig.extract(Map(ERROR_POLICY_KEY -> "conTinue")).map(_.policyType) shouldBe Right(
      ErrorPolicyType.CONTINUE,
    )
  }

  test(s"it parses $ERROR_CONTINUE_ON_INVALID_INPUT_KEY") {
    val testCases = List(
      Map(ERROR_POLICY_KEY -> "retry")
        -> ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 60000), continueOnInvalidInput = false),
      Map(ERROR_POLICY_KEY -> "retry", ERROR_CONTINUE_ON_INVALID_INPUT_KEY -> "false")
        -> ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 60000), continueOnInvalidInput = false),
      Map(ERROR_POLICY_KEY -> "retry", ERROR_CONTINUE_ON_INVALID_INPUT_KEY -> "true")
        -> ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 60000), continueOnInvalidInput = true),
    )

    testCases.foreach { case (props, expectedConfig) =>
      assertResult(Right(expectedConfig))(ErrorPolicyConfig.extract(props))
    }
  }

  test("build Error Policy based on config") {
    val testCases = List(
      ErrorPolicyConfig(ErrorPolicyType.CONTINUE, RetryConfig(10, 60000), false) -> Continue,
      ErrorPolicyConfig(ErrorPolicyType.THROW, RetryConfig(10, 60000), false)    -> Throw,
      ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 60000), false)    -> Retry,
      ErrorPolicyConfig(ErrorPolicyType.CONTINUE, RetryConfig(10, 60000), true) -> ContinueOnInvalidInput(
        Continue,
      ),
      ErrorPolicyConfig(ErrorPolicyType.THROW, RetryConfig(10, 60000), true) -> ContinueOnInvalidInput(Throw),
      ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 60000), true) -> ContinueOnInvalidInput(Retry),
    )

    testCases.foreach { case (config, expectedPolicy) =>
      config.errorPolicy shouldBe expectedPolicy
    }
  }
}
