/*
 * Copyright 2017-2022 Celonis Ltd
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
