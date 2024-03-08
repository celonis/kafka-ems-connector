/*
 * Copyright 2024 Celonis SE
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

package com.celonis.kafka.connect.ems.errors

import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import scala.util.Try

class InvalidInputErrorHandlerTest extends AnyFunSuite with Matchers {
  test("it does not throw for InvalidInputError when flag set to true") {
    val handler = new InvalidInputErrorHandler(true, None)
    noException should be thrownBy handler.handle(aSinkRecord, InvalidInputException("oooouuups"))
  }

  test("it throws for InvalidInputError when flag set to false") {
    val handler   = new InvalidInputErrorHandler(false, None)
    val exception = InvalidInputException("oooouuups")
    the[InvalidInputException] thrownBy handler.handle(aSinkRecord, exception) shouldBe exception
  }

  test("it throws for other exceptions, regardless of flag value") {
    val handler1  = new InvalidInputErrorHandler(false, None)
    val handler2  = new InvalidInputErrorHandler(true, None)
    val exception = new RuntimeException("oooouuups")

    the[RuntimeException] thrownBy handler1.handle(aSinkRecord, exception) shouldBe exception
    the[RuntimeException] thrownBy handler2.handle(aSinkRecord, exception) shouldBe exception
  }

  test("it should report only InvalidInputError when flag set to true") {
    val reporter  = new FakeErrorReporter
    val exception = InvalidInputException("oooouuups")

    val handler = new InvalidInputErrorHandler(true, Some(reporter))
    handler.handle(aSinkRecord, exception)
    Try(handler.handle(aSinkRecord, new RuntimeException("ignored")))
    reporter.getErrors shouldBe List(aSinkRecord -> exception)
  }

  test("it should never report if flag set to false") {
    val reporter  = new FakeErrorReporter
    val exception = InvalidInputException("oooouuups")

    val handler = new InvalidInputErrorHandler(false, Some(reporter))
    Try(handler.handle(aSinkRecord, exception))
    Try(handler.handle(aSinkRecord, new RuntimeException("ignored")))
    reporter.getErrors shouldBe Nil
  }

  private lazy val aSinkRecord = new SinkRecord("topic", 0, null, "", null, "", 0)

  final class FakeErrorReporter() extends ErrantRecordReporter {
    private var errors: List[(SinkRecord, Throwable)] = Nil

    def getErrors: List[(SinkRecord, Throwable)] = errors

    override def report(record: SinkRecord, error: Throwable): Future[Void] = {
      errors = (record, error) :: errors
      CompletableFuture.completedFuture(null)
    }

  }
}
