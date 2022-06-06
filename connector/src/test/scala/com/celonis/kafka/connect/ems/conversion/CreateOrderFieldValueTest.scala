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

package com.celonis.kafka.connect.ems.conversion

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CreateOrderFieldValueTest extends AnyFunSuite with Matchers {
  test("preserve the order for a given partition") {
    val p00 = CreateOrderFieldValue(0, 0)
    val p01 = CreateOrderFieldValue(0, 1)
    p00 < p01 shouldBe true
  }
  test("higher partition means the same order number") {
    val p00 = CreateOrderFieldValue(0, 0)
    val p10 = CreateOrderFieldValue(1, 0)
    p00 shouldBe p10
  }
  test("preserve the order for a high number partition") {
    val p00 = CreateOrderFieldValue(1000, 0)
    val p01 = CreateOrderFieldValue(1000, 1)
    p00 < p01 shouldBe true
  }
}
