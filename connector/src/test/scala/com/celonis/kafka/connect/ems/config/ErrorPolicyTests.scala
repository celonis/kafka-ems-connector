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

import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Continue
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Throw
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ErrorPolicyTests extends AnyFunSuite with Matchers {

  test(s"handle an error according to the configured error policy") {
    val throwable = new RuntimeException()
    // retry
    an[RetriableException] should be thrownBy Retry.handle(throwable, 1)
    an[ConnectException] should be thrownBy Retry.handle(throwable, 0)
    // throw
    an[ConnectException] should be thrownBy Throw.handle(throwable, 10)
    // continue
    noException should be thrownBy Continue.handle(throwable, 10)
  }
}
