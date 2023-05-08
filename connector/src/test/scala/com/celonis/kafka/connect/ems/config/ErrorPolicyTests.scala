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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Continue
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Throw
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException

class ErrorPolicyTests extends AnyFunSuite with Matchers {
  test(s"return an error if $ERROR_POLICY_KEY is missing") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicy.extract(Map.empty) shouldBe Left(expectedMessage)
    ErrorPolicy.extract(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    ErrorPolicy.extract(Map("a" -> "b", ERROR_POLICY_KEY + ".ext" -> 1)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is empty") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is not a string") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ERROR_POLICY_KEY is invalid") {
    val expectedMessage = s"Invalid [$ERROR_POLICY_KEY]. $ERROR_POLICY_DOC"
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "retry.")) shouldBe Left(expectedMessage)
  }

  test(s"return the target table provided with $ERROR_POLICY_KEY") {
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "retry")) shouldBe Right(Retry)
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "throw")) shouldBe Right(Throw)
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "continue")) shouldBe Right(Continue)

    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "rEtry")) shouldBe Right(Retry)
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "THROW")) shouldBe Right(Throw)
    ErrorPolicy.extract(Map(ERROR_POLICY_KEY -> "conTinue")) shouldBe Right(Continue)
  }

  test(s"handle an error according to the configured error policy") {
    val throwable = new RuntimeException()
    // retry
    an[RetriableException] should be thrownBy Retry.handle(throwable, 1)
    an[ConnectException] should be thrownBy Retry.handle(throwable, 0)
    // throw
    an[ConnectException] should be thrownBy Throw.handle(throwable, 10)
    // continue
    noException should be thrownBy Continue.handle(throwable, 10)
  }
}
