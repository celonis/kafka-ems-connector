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

package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import com.celonis.kafka.connect.transform.flatten.ConnectJsonConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters._

class InferSchemaAndNormaliseValueTest extends org.scalatest.funsuite.AnyFunSuite with Matchers {
  test("returns None when encountering an unexpected value") {
    List[Any](
      null,
      Range(1, 10),
      Iterator.continually(true),
      (),
    ).foreach {
      value => assertResult(None)(InferSchemaAndNormaliseValue(value))
    }
  }
  test("Infers the schema of simple primitives") {
    List(
      "hi"  -> Schema.OPTIONAL_STRING_SCHEMA,
      12L   -> Schema.OPTIONAL_INT64_SCHEMA,
      15.2d -> Schema.OPTIONAL_FLOAT64_SCHEMA,
      true  -> Schema.OPTIONAL_BOOLEAN_SCHEMA,
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(ValueAndSchema(value, expectedSchema)))(InferSchemaAndNormaliseValue(value))
    }
  }

  test("Infers simple maps of strings") {
    val value          = Map("hi" -> "there").asJava
    val expectedSchema = SchemaBuilder.struct().field("hi", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedValue  = new Struct(expectedSchema).put("hi", "there")

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(InferSchemaAndNormaliseValue(value))
  }

  test("Infers simple maps of primitives") {
    val value          = Map(1L -> true).asJava
    val expectedSchema = SchemaBuilder.struct().field("1", Schema.OPTIONAL_BOOLEAN_SCHEMA).build()
    val expectedValue  = new Struct(expectedSchema).put("1", true)

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(InferSchemaAndNormaliseValue(value))
  }

  test("Infers simple collections") {
    val value          = List("a", "b", "c").asJava
    val expectedSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build()

    assertResult(Some(ValueAndSchema(value, expectedSchema)))(InferSchemaAndNormaliseValue(value))
  }

  test("Normalisation transforms maps nested in maps") {
    val value          = Map("nested" -> Map("a" -> "123").asJava).asJava
    val nestedSchema   = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedSchema = SchemaBuilder.struct().field("nested", nestedSchema).build()
    val expectedValue  = new Struct(expectedSchema).put("nested", new Struct(nestedSchema).put("a", "123"))

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(InferSchemaAndNormaliseValue(value))
  }

  test("Normalisation transforms maps nested in arrays") {
    val value          = List(Map("a" -> "123").asJava).asJava
    val nestedSchema   = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedSchema = SchemaBuilder.array(nestedSchema).build()
    val expectedValue  = List(new Struct(nestedSchema).put("a", "123")).asJava

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(InferSchemaAndNormaliseValue(value))
  }

  test("Succeeds with empty collections") {
    List(
      Map.empty[Boolean, Boolean].asJava -> SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build(),
      List.empty[Int].asJava             -> SchemaBuilder.array(Schema.BYTES_SCHEMA).build(),
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(ValueAndSchema(value, expectedSchema)))(InferSchemaAndNormaliseValue(value))
    }
  }

  test("Fails with non-empty heterogeneous collections") {
    List(
      List[Any](1, "blah", true).asJava,
      List(List[Any](1, "blah")).asJava,
    ).foreach { value =>
      assertResult(None)(InferSchemaAndNormaliseValue(value))
    }
  }

  test("Fails when a map contains an empty key") {
    val value = Map("" -> "x", "y" -> "x").asJava
    assertResult(None)(InferSchemaAndNormaliseValue(value))
  }

  test("infers nested object's schema") {
    val rawJson =
      """
        |{"hello": {"I": "am_a_nested", "object": true, "with": ["an","array", "of-strings"]}}
        |""".stripMargin
    val om     = new ObjectMapper()
    val value  = om.readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val schema = InferSchemaAndNormaliseValue(value).map(_.schema).getOrElse(fail("some schema expected!"))

    assertResult(
      SchemaBuilder.struct().field(
        "hello",
        SchemaBuilder.struct()
          .field("I", Schema.OPTIONAL_STRING_SCHEMA)
          .field("object", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("with", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
          .build(),
      ).build(),
    )(schema)
  }

  test("omits fields with a null value") {
    val rawJson =
      """
        |{"hello": {"f1": true, "omit_me_iam_a_null_value": null}}
        |""".stripMargin

    val value          = new ObjectMapper().readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val valueAndSchema = InferSchemaAndNormaliseValue(value).getOrElse(fail("some schema expected!"))

    val expectedSchema = SchemaBuilder.struct().field(
      "hello",
      SchemaBuilder.struct()
        .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build(),
    ).build()

    val expectedValue =
      new Struct(expectedSchema).put("hello", new Struct(expectedSchema.field("hello").schema()).put("f1", true))

    assertResult(expectedSchema)(valueAndSchema.schema)
    assertResult(expectedValue)(valueAndSchema.normalisedValue)
  }

  test("normalises a schemaless JSON") {
    val json =
      """
        |{
        |  "idType": 3,
        |  "colorDepth": "",
        |  "threshold" : 45.77,
        |  "evars": {
        |    "evarsa": {
        |      "eVar1": "Tue Aug 27 2019 12:08:10",
        |      "eVar2": 1566922079
        |    }
        |  },
        |  "exclude": {
        |    "id": 0,
        |    "value": false
        |  },
        |  "cars":[ "Ford", "BMW", "Fiat" ],
        |  "nums": [ 1, 3, 4 ]
        |  }
        |}
        |""".stripMargin

    val schemaAndValue                           = ConnectJsonConverter.converter.toConnectData("topic", json.getBytes)
    val Some(ValueAndSchema(normalisedValue, _)) = InferSchemaAndNormaliseValue(schemaAndValue.value())
    val struct                                   = normalisedValue.asInstanceOf[Struct]

    assertResult(Set(
      "idType",
      "colorDepth",
      "threshold",
      "evars",
      "exclude",
      "cars",
      "nums",
    ))(struct.schema().fields().asScala.map(_.name()).toSet)

    struct.get("idType") shouldBe 3
    struct.get("colorDepth") shouldBe ""
    struct.get("threshold") shouldBe 45.77
    val evars = struct.get("evars").asInstanceOf[Struct]
    evars.schema().fields().asScala.map(_.name()).toList.sorted shouldBe List("evarsa")
    val evarsa = evars.get("evarsa").asInstanceOf[Struct]
    evarsa.schema.fields.asScala.map(_.name()).toList.sorted shouldBe List("eVar1", "eVar2")
    evarsa.get("eVar1") shouldBe "Tue Aug 27 2019 12:08:10"
    evarsa.get("eVar2") shouldBe 1566922079L
    val exclude = struct.get("exclude").asInstanceOf[Struct]
    exclude.schema.fields.asScala.map(_.name()).toList.sorted shouldBe List("id", "value")
    exclude.get("id") shouldBe 0
    exclude.get("value") shouldBe false

    struct.get("cars").asInstanceOf[java.util.List[AnyRef]].asScala.toArray shouldBe Array("Ford", "BMW", "Fiat")
    struct.get("nums").asInstanceOf[java.util.List[AnyRef]].asScala.toArray shouldBe Array(1, 3, 4)
  }
}
