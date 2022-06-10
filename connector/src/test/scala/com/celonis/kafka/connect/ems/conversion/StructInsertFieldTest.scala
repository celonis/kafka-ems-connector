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

package com.celonis.kafka.connect.ems.conversion

import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StructInsertFieldTest extends AnyFunSuite with Matchers {
  test("add a new field to a struct") {
    val converter = new AvroData(1)
    val policy    = Policy(1, PolicyOwner("me"), 1.1)
    val record    = Policy.rf.to(policy)
    val input     = converter.toConnectData(SchemaFor[Policy].schema, record)
    val expected  = 1111111L
    val output    = StructInsertField.apply(input.value().asInstanceOf[Struct], OrderFieldInserter.FieldName, expected)
    output.get(OrderFieldInserter.FieldName) shouldBe expected
  }
}

case class PolicyOwner(name: String)

object PolicyOwner {
  implicit val rf: RecordFormat[PolicyOwner] = RecordFormat[PolicyOwner]
}

case class Policy(number: Int, owner: PolicyOwner, amount: Double)

object Policy {
  implicit val rf: RecordFormat[Policy] = RecordFormat[Policy]
}
