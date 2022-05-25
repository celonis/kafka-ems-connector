/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA1
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Sha1ObfuscationTest extends AnyFunSuite with Matchers {
  private val obfuscation = SHA1

  test("null returns null") {
    obfuscation.obfuscate(null) shouldBe null
  }

  test("returns the expected value") {
    obfuscation.obfuscate("") should not equal ""
    obfuscation.obfuscate("this is a test") shouldBe "9938a75e6d10a74d6b2e9bc204177de5b95f28fe"
  }
}
