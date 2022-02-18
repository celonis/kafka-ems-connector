/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FixObfuscationTest extends AnyFunSuite with Matchers {
  private val obfuscation = FixObfuscation(5, '*')

  test("null returns null") {
    obfuscation.obfuscate(null) shouldBe null
  }

  test("returns the expected value") {
    obfuscation.obfuscate("null") shouldBe obfuscation.constant
    obfuscation.obfuscate("") shouldBe obfuscation.constant
    obfuscation.obfuscate(" ") shouldBe obfuscation.constant
  }

}
