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

package com.celonis.kafka.connect.ems.storage.formats

import cats.data.NonEmptySeq
import org.apache.avro.generic.GenericRecord
import org.scalatestplus.mockito.MockitoSugar

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NoOpExploderTest extends AnyFunSuite with Matchers with MockitoSugar {

  test("returns a singleton list") {
    val genericRecord = mock[GenericRecord]
    val noOpExploder  = new NoOpExploder()
    noOpExploder.explode(genericRecord) should be(NonEmptySeq.one(genericRecord))
  }

}
