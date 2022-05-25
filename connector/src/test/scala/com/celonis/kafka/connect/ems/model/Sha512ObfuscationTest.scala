/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithSalt
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class Sha512ObfuscationTest extends AnyFunSuite with Matchers {
  private val obfuscation = SHA512WithSalt("bamboo".getBytes(StandardCharsets.UTF_8))

  test("null returns null") {
    obfuscation.obfuscate(null) shouldBe null
  }

  test("returns the expected value") {
    obfuscation.obfuscate("") should not equal ""
    obfuscation.obfuscate("this is a test") shouldBe "4b8f3b21e26a40e81db1c02380f994e2ba836d1eed933f4455a3e04c2b2762da26333c50b5a6fd2deb2b385c857cfe1805d995085d3a3f9365b63a4b00679774"
  }
}
