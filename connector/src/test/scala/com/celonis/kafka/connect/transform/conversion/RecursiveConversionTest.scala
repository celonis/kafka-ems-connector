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

package com.celonis.kafka.connect.transform.conversion

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

import java.time.Instant

class RecursiveConversionTest extends AnyFunSuite with Matchers {
  test("it is  recursive on structs") {
    val nestedSchema = SchemaBuilder.struct().field("a_timestamp", Timestamp.SCHEMA).build()
    val nestedStruct = new Struct(nestedSchema)
    nestedStruct.put("a_timestamp", aDate)

    val originalSchema = SchemaBuilder.struct()
      .field("an_int", Schema.INT32_SCHEMA)
      .field("nested", nestedSchema)
      .build()

    val originalValue = new Struct(originalSchema)
    originalValue.put("an_int", 123)
    originalValue.put("nested", nestedStruct)

    val expectedNestedSchema = SchemaBuilder.struct().field("a_timestamp", Schema.STRING_SCHEMA).build()
    val expectedSchema = SchemaBuilder.struct()
      .field("an_int", Schema.STRING_SCHEMA)
      .field("nested", expectedNestedSchema)
      .build()

    val expectedNestedValue = new Struct(expectedNestedSchema)
    expectedNestedValue.put("a_timestamp", aDate.toString)

    val expectedValue = new Struct(expectedSchema)
    expectedValue.put("an_int", "123")
    expectedValue.put("nested", expectedNestedValue)

    val (convertedValue, Some(convertedSchema)) = recursiveConversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe expectedSchema
    convertedValue shouldBe expectedValue
  }

  test("it is  recursive on maps") {
    val originalSchema = SchemaBuilder.map(
      Schema.FLOAT64_SCHEMA,
      SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT64_SCHEMA).build(),
    ).build()

    val originalValue = Map(1.23d -> Map(123 -> 456L).asJava).asJava

    val expectedSchema = SchemaBuilder.map(
      Schema.STRING_SCHEMA,
      SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(),
    ).build()

    val expectedValue = Map(1.23d.toString -> Map(123.toString -> 456L.toString).asJava).asJava

    val (convertedValue, Some(convertedSchema)) = recursiveConversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe expectedSchema
    convertedValue shouldBe expectedValue
  }

  test("it is  recursive on collections") {
    val originalSchema = SchemaBuilder.array(
      SchemaBuilder.array(Schema.INT32_SCHEMA).build(),
    ).build()

    val originalValue = List(List(1, 2).asJava, List(3, 4).asJava).asJava

    val expectedSchema = SchemaBuilder.array(
      SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
    ).build()

    val expectedValue = List(List("1", "2").asJava, List("3", "4").asJava).asJava

    val (convertedValue, Some(convertedSchema)) = recursiveConversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe expectedSchema
    convertedValue shouldBe expectedValue
  }

  lazy val recursiveConversion = new RecursiveConversion(stringConversion)

  lazy val stringConversion = new ConnectConversion {
    override def convertSchema(originalSchema: Schema): Schema = Schema.STRING_SCHEMA
    override def convertValue(connectValue: Any, originalSchema: Schema, targetSchema: Schema): Any =
      connectValue.toString
  }

  lazy val aDate = java.util.Date.from(Instant.now())
}
