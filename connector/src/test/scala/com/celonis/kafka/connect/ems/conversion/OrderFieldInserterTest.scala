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

import cats.syntax.either._
import cats.syntax.option._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util

class OrderFieldInserterTest extends AnyFunSuite with Matchers {
  test("raises an exception when the payload is not Struct/Map") {
    OrderFieldInserter.add(1L, 0, 1) shouldBe InvalidInputException(
      s"Invalid input received. To write the data to Parquet files the input needs to be an Object but found:java.lang.Long.",
    ).asLeft
  }

  test("adds a field to a Connect Struct") {
    val converter = new AvroData(1)
    val policy    = Policy(1, PolicyOwner("me"), 1.1)
    val record    = RecordFormat[Policy].to(policy)
    val input     = converter.toConnectData(SchemaFor[Policy].schema, record)
    val expected  = CreateOrderFieldValue(0, 1)
    val output    = OrderFieldInserter.add(input.value(), 0, 1).getOrElse(fail("expected to work")).asInstanceOf[Struct]
    output.get(OrderFieldInserter.FieldName) shouldBe expected
  }

  test("adds a field to a Map") {
    val input: Map[String, Any] = Map("a" -> "1", "b" -> 1L)
    val expected = CreateOrderFieldValue(0, 1)
    val output   = OrderFieldInserter.add(input, 0, 1).getOrElse(fail("expected to work")).asInstanceOf[Map[String, _]]
    output.get(OrderFieldInserter.FieldName) shouldBe expected.some
  }

  test("adds a field to a java Map") {
    val input: util.Map[String, Any] = new util.HashMap[String, Any]()
    input.put("a", "1")
    input.put("b", 1L)
    val expected = CreateOrderFieldValue(0, 1)
    val output =
      OrderFieldInserter.add(input, 0, 1).getOrElse(fail("expected to work")).asInstanceOf[util.Map[String, _]]
    output.get(OrderFieldInserter.FieldName) shouldBe expected
  }
}
