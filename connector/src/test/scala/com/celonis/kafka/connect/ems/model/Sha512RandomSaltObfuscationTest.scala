/*
 * Copyright 2017-2022 Celonis Ltd
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
