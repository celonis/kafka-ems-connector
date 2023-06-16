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

package com.celonis.kafka.connect.transform.flatten

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import scala.collection.mutable

class SchemaFlattenerTest extends org.scalatest.funsuite.AnyFunSuite {
  test("flattens a schema making all primitives optional") {

    primitiveFixtures.foreach {
      primitiveSchema =>
        val schema = SchemaBuilder.struct()
          .field("a_primitive", primitiveSchema.build())
          .field(
            "nested",
            SchemaBuilder.struct().field(
              "deeper",
              SchemaBuilder.struct().field("a_bool", Schema.BOOLEAN_SCHEMA).build(),
            ).build(),
          )
          .build()

        val expected: Schema = SchemaBuilder
          .struct()
          .field("a_primitive", primitiveSchema.optional().build())
          .field("nested_deeper_a_bool", SchemaBuilder.bool().optional().build())
          .build()

        withClue(s"expected schema fields ${expected.fields()} for primitive ${primitiveSchema.build()}") {
          assertResult(expected)(flatten(schema))
        }
    }
  }

  test("turns arrays and maps into optional string fields") {
    val schema = SchemaBuilder.struct()
      .field("an_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
      .field(
        "nested",
        SchemaBuilder.struct().field(
          "deeper",
          SchemaBuilder.struct().field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT16_SCHEMA)).schema(),
        ).build(),
      )
      .build()

    val expected = SchemaBuilder.struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field(
        "nested_deeper_a_map",
        Schema.OPTIONAL_STRING_SCHEMA,
      )
      .build()

    assertResult(expected)(flatten(schema))
  }

  test("drops arrays/maps when discardCollections is set") {
    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a_nested_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build())
      .field("a_nested_array", SchemaBuilder.array(SchemaBuilder.string()).build())
      .field("a_bool", SchemaBuilder.bool().build())
      .build()

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .field("an_array", SchemaBuilder.array(SchemaBuilder.string()).schema())
      .field("a_struct", nestedSchema)
      .build()

    val expected = SchemaBuilder
      .struct()
      .field("a_string", SchemaBuilder.string().optional().build())
      .field("a_struct_a_bool", SchemaBuilder.bool().optional().build())
      .build()

    assertResult(expected)(flatten(schema, true))
  }

  test("leaves a top-level collection untouched even when discardCollections is set") {
    collectionFixtures.foreach {
      case (_, schema) =>
        withClue(s"expected $schema to be unchanged") {
          assertResult(schema)(flatten(schema, true))
        }
    }
  }

  test("sanitises keys as AVRO compliant field names") {
    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a nested!field", Schema.STRING_SCHEMA)
      .field("a?nested string", Schema.STRING_SCHEMA)
      .build()

    val schema = SchemaBuilder.struct()
      .field("a#struct", nestedSchema)
      .build()

    val expected = SchemaBuilder
      .struct()
      .field("a_struct_a_nested_field", SchemaBuilder.string().optional().build())
      .field("a_struct_a_nested_string", SchemaBuilder.string().optional().build())
      .build()

    assertResult(expected)(flatten(schema))
  }

  lazy val primitiveFixtures = List[SchemaBuilder](
    SchemaBuilder.int8(),
    SchemaBuilder.int16(),
    SchemaBuilder.int32(),
    SchemaBuilder.int64(),
    SchemaBuilder.float32(),
    SchemaBuilder.float64(),
    SchemaBuilder.bool(),
    SchemaBuilder.string(),
    SchemaBuilder.bytes(),
    org.apache.kafka.connect.data.Date.builder(),
    org.apache.kafka.connect.data.Time.builder(),
    org.apache.kafka.connect.data.Timestamp.builder(),
    org.apache.kafka.connect.data.Decimal.builder(24),
  )

  lazy val collectionFixtures = List(
    mutable.HashMap("hello" -> true) -> SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build(),
    List("hello", "world")           -> SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
  )

  private def flatten(schema: Schema, discardCollections: Boolean = false): Schema =
    new SchemaFlattener(discardCollections).flatten(schema).connectSchema
}
