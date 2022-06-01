/*
 * Copyright 2022 Celonis SE
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

package com.celonis.kafka.connect.ems.model

import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithRandomSalt
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Sha512RandomSaltObfuscationTest extends AnyFunSuite with Matchers {
  private val obfuscation = SHA512WithRandomSalt()

  test("null returns null") {
    obfuscation.obfuscate(null) shouldBe null
  }

  test("rotates the values") {
    obfuscation.obfuscate("") should not equal ""
    val obWithSalt1 = obfuscation.obfuscate("this is a test")
    val obWithSalt2 = obfuscation.obfuscate("this is a test")
    obWithSalt1 should not equal "this is a test"
    obWithSalt2 should not equal obWithSalt1
    obWithSalt1.forall(c => c.isDigit || c.isValidChar) shouldBe true
  }
}
