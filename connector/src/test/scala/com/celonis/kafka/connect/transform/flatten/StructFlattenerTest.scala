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

import com.celonis.kafka.connect.ems.storage.SampleData
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.joda.time.LocalDate
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class StructFlattenerTest extends AnyFunSuite with SampleData {

  test("do nothing on a primitive") {

    primitiveValuesAndSchemas.foreach {
      primitiveValueAndSchema =>
        val result = flatten(primitiveValueAndSchema.avroValue, primitiveValueAndSchema.connectSchema)
        assertResult(result)(primitiveValueAndSchema.avroValue)
    }
  }

  test("flattens nested fields") {

    val structOfPrimitivesSchema = primitiveValuesAndSchemas.foldLeft(SchemaBuilder.struct()) {
      case (builder, valueAndSchemas) => builder.field(valueAndSchemas.name, valueAndSchemas.connectSchema)
    }.build()

    val structOfPrimitives = new Struct(structOfPrimitivesSchema)
    primitiveValuesAndSchemas.foreach { valueAndSchemas =>
      structOfPrimitives.put(valueAndSchemas.name, valueAndSchemas.connectValue)
    }

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("x", structOfPrimitivesSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("x", structOfPrimitives)

    val expectedFlatSchema = SchemaBuilder.struct().field("a_string", SchemaBuilder.string().optional().build())
    primitiveValuesAndSchemas.foreach { valueAndSchemas =>
      expectedFlatSchema.field("x_" + valueAndSchemas.name, valueAndSchemas.optionalConnectSchema)
    }

    val result = flatten(struct, schema).asInstanceOf[Struct]

    assertResult(expectedFlatSchema.build())(result.schema())
    assertResult("hello")(result.get("a_string"))

    primitiveValuesAndSchemas.foreach { valueAndSchemas =>
      withClue(valueAndSchemas) {
        assertResult(valueAndSchemas.connectValue)(result.get("x_" + valueAndSchemas.name))
      }
    }
    assertThrows[DataException](result.get("x"))
  }

  test("transforms arrays and maps of primitives into strings") {
    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("an_array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
      .build()

    val nested = new Struct(nestedSchema)
    nested.put("an_array", List(1, 2, 3).asJava)
    nested.put("a_map", Map("1" -> "a", "2" -> "b").asJava)

    val schema = SchemaBuilder.struct()
      .field("nested", nestedSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("nested", nested)

    val flatSchema = SchemaBuilder
      .struct()
      .field("nested_an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("nested_a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val mapper = new ObjectMapper()

    val result = flatten(struct, schema).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())

    assertResult(mutable.Map("1" -> "a", "2" -> "b")) {
      mapper.readValue(result.getString("nested_a_map"), classOf[java.util.Map[String, String]]).asScala
    }
    assertResult(mutable.Buffer(1, 2, 3)) {
      mapper.readValue(result.getString("nested_an_array"), classOf[java.util.LinkedList[String]]).asScala
    }
  }

  test("JSON encodes collection of AVRO records") {
    val nestedSchema = SchemaBuilder.struct()
      .field("a_bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("a_long", Schema.OPTIONAL_INT64_SCHEMA)
      .build()

    val nested = new Struct(nestedSchema)
    nested.put("a_bool", true)
    nested.put("a_long", 33L)

    val schema = SchemaBuilder.struct()
      .field("an_array", SchemaBuilder.array(nestedSchema))
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, nestedSchema))
      .build()

    val struct = new Struct(schema)
    struct.put("an_array", List(nested).asJava)
    struct.put("a_map", Map("key" -> nested).asJava)

    val flatSchema = SchemaBuilder
      .struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val result = flatten(struct, schema).asInstanceOf[Struct]
    assertResult("""[{"a_bool":true,"a_long":33}]""")(result.get("an_array"))
    assertResult(flatSchema)(result.schema())
    assertResult("""{"key":{"a_bool":true,"a_long":33}}""")(result.get("a_map"))
  }

  test("JSON encodes collection of JSON records") {

    val nestedSchema = SchemaBuilder.struct()
      .field("a_bool", Schema.BOOLEAN_SCHEMA)
      .field("a_long", Schema.INT64_SCHEMA)
      .build()

    val schema = SchemaBuilder
      .struct()
      .field("an_array", SchemaBuilder.array(nestedSchema).build())
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, nestedSchema).build())
      .build()

    val flatSchema = SchemaBuilder
      .struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val nested = Map[String, Any](
      "a_bool" -> true,
      "a_long" -> 33,
    ).asJava

    val jsonRecord = Map[String, Any](
      "an_array" -> List(nested).asJava,
      "a_map"    -> Map("key" -> nested).asJava,
    ).asJava

    val result = flatten(jsonRecord, schema).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("""[{"a_bool":true,"a_long":33}]""")(result.get("an_array"))
    assertResult("""{"key":{"a_bool":true,"a_long":33}}""")(result.get("a_map"))
  }

  test("drops arrays/maps when 'discardCollections' is set") {
    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a_nested_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build())
      .field("a_nested_array", SchemaBuilder.array(SchemaBuilder.string()).build())
      .field("a_bool", SchemaBuilder.bool().build())
      .build()
    val nested = new Struct(nestedSchema)
    nested.put("a_nested_map", mutable.HashMap("x" -> "y").asJava)
    nested.put("a_nested_array", List("blah").asJava)
    nested.put("a_bool", true)

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .field("an_array", SchemaBuilder.array(SchemaBuilder.string()).schema())
      .field("a_struct", nestedSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", mutable.HashMap("hello" -> "hi-there...").asJava)
    struct.put("an_array", List("discard", "me", "please").asJava)
    struct.put("a_struct", nested)

    val flatSchema = SchemaBuilder
      .struct()
      .field("a_string", SchemaBuilder.string().optional().build())
      .field("a_struct_a_bool", SchemaBuilder.bool().optional().build())
      .build()

    val result = flatten(struct, schema, true).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("a_struct_a_bool"))

    assertThrows[DataException](result.get("a_struct"))
    assertThrows[DataException](result.get("a_map"))
    assertThrows[DataException](result.get("an_array"))
  }

  test("leaves top level collections untouched when 'discardCollections' is set") {
    case class TestData(label: String, value: AnyRef, flattenedSchema: Schema)

    val mapValue:   java.util.Map[String, Int] = mutable.HashMap("x" -> 22).asJava
    val arrayValue: java.util.List[String]     = List("a", "b", "c").asJava

    val testData = List(
      TestData(
        "an map in top-level position",
        mapValue,
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
      ),
      TestData("an array in top-level position", arrayValue, SchemaBuilder.array(Schema.STRING_SCHEMA).build()),
    )
    testData.foreach {
      case TestData(label, value, schema) =>
        withClue(s"$label : $value") {
          assertResult(value) {
            flatten(value, schema, true)
          }
        }
    }
  }

  test("when the schema is inferred, flattens nested maps instead than json-encoding them") {
    val nestedMap = Map(
      "some" -> Map(
        "nested-string" -> "a-string",
        "nested-array"  -> List("a", "b", "c").asJava,
        "nested-map"    -> Map[String, Any]("one-more-level" -> true).asJava,
      ).asJava,
    ).asJava

    val schema = SchemaBuilder.struct()
      .field(
        "some",
        SchemaBuilder.struct()
          .field("nested-string", Schema.OPTIONAL_STRING_SCHEMA)
          .field("nested-array", Schema.OPTIONAL_STRING_SCHEMA)
          .field("nested-map",
                 SchemaBuilder.struct()
                   .field("one-more-level", Schema.OPTIONAL_BOOLEAN_SCHEMA).build(),
          )
          .build(),
      ).build()

    val flattenedSchema = SchemaBuilder.struct()
      .field("some_nested_string", Schema.OPTIONAL_STRING_SCHEMA)
      .field("some_nested_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("some_nested_map_one_more_level", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build()

    val expected = new Struct(flattenedSchema)

    expected.put("some_nested_string", "a-string")
    expected.put("some_nested_array", """["a","b","c"]""")
    expected.put("some_nested_map_one_more_level", true)

    assertResult(expected)(flatten(nestedMap, schema))
  }

  lazy val aDecimal:             java.math.BigDecimal = new java.math.BigDecimal(java.math.BigInteger.valueOf(123), 5)
  lazy val decimalConnectSchema: SchemaBuilder        = Decimal.builder(5).parameter("connect.decimal.precision", "24")

  lazy val aDate = LocalDate.now()

  private def flatten(value: Any, schema: Schema, discardCollections: Boolean = false): Any =
    StructFlattener.flatten(value, new SchemaFlattener(discardCollections).flatten(schema))

}
