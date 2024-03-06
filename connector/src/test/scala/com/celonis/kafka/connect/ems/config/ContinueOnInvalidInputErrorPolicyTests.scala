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

import com.celonis.kafka.connect.ems.errors.InvalidInputException
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.ContinueOnInvalidInputPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Throw
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ContinueOnInvalidInputErrorPolicyTests extends AnyFunSuite with Matchers {
  test("Does not call inner policy if error is an InvalidInputException") {
    val policy = new ContinueOnInvalidInputPolicy(Throw)

    noException should be thrownBy policy.handle(InvalidInputException("Ooops"), 3)
  }

  test("Call inner policy if error is not an InvalidInputException") {
    val policy = new ContinueOnInvalidInputPolicy(Throw)

    assertThrows[RuntimeException](policy.handle(new RuntimeException("Ooops"), 3))
  }
}
