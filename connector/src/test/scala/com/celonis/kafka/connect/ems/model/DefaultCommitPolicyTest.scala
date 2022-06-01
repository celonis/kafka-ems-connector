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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class DefaultCommitPolicyTest extends AnyFunSuite with Matchers {
  test("roll over after file size") {
    val policy = DefaultCommitPolicy(10, 1.minutes.toMillis, 10000)
    policy.shouldFlush(CommitContext(10, 10, System.currentTimeMillis())) shouldBe true
  }

  test("trigger flush once records count is reached") {
    val policy = DefaultCommitPolicy(10, 1.minutes.toMillis, 10000)
    policy.shouldFlush(CommitContext(10000, 9, System.currentTimeMillis())) shouldBe true
  }

  test("trigger flush when time limit is reached") {
    val policy = DefaultCommitPolicy(10, 1.minute.toMillis, 10000)
    policy.shouldFlush(CommitContext(999, 9, System.currentTimeMillis() - 1.minutes.toMillis)) shouldBe true
  }

  test("nothing to flush") {
    val policy = DefaultCommitPolicy(10, 1.minute.toMillis, 10000)
    policy.shouldFlush(CommitContext(999, 9, System.currentTimeMillis() - 1.minutes.toMillis + 100)) shouldBe false
  }
}
