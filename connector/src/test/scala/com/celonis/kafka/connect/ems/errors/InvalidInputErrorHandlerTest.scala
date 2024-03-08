package com.celonis.kafka.connect.ems.errors

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InvalidInputErrorHandlerTest extends AnyFunSuite with Matchers {
  test("it does not throw for InvalidInputError when flag set to true") {
    val handler = new InvalidInputErrorHandler(true)
    noException should be thrownBy handler.handle(InvalidInputException("oooouuups"))
  }

  test("it throws for InvalidInputError when flag set to false") {
    val handler   = new InvalidInputErrorHandler(false)
    val exception = InvalidInputException("oooouuups")
    the[InvalidInputException] thrownBy handler.handle(exception) shouldBe exception
  }

  test("it throws for other exceptions, regardless of flag value") {
    val handler1  = new InvalidInputErrorHandler(false)
    val handler2  = new InvalidInputErrorHandler(true)
    val exception = new RuntimeException("oooouuups")

    the[RuntimeException] thrownBy handler1.handle(exception) shouldBe exception
    the[RuntimeException] thrownBy handler2.handle(exception) shouldBe exception
  }
}
