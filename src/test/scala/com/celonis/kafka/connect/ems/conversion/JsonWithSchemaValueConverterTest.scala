/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class JsonWithSchemaValueConverterTest extends AnyFunSuite with Matchers {
  private val converter = new org.apache.kafka.connect.json.JsonConverter()
  converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                          "converter.type"                          -> "value",
                          "schemas.enable"                          -> "true",
  ).asJava)

  test("convert struct") {
    val json =
      """
        |{
        |  "schema": {
        |    "type": "struct",
        |    "fields": [
        |      {
        |        "field": "field1",
        |        "type": "boolean"
        |      },
        |      {
        |        "field": "field2",
        |        "type": "string"
        |      }
        |    ]
        |  },
        |  "payload": {
        |    "field1": true,
        |    "field2": "string"
        |  }
        |}""".stripMargin

    val schemaAndValue = converter.toConnectData("topic", json.getBytes)
    val struct         = ValueConverter.apply(schemaAndValue.value()).getOrElse(fail("Should convert the map"))
    struct.schema().fields().asScala.map(_.name()).sorted shouldBe List("field1", "field2").sorted

    struct.schema().field("field1").schema() shouldBe Schema.BOOLEAN_SCHEMA
    struct.schema().field("field2").schema() shouldBe Schema.STRING_SCHEMA

    struct.get("field1") shouldBe true
    struct.get("field2") shouldBe "string"
  }

  test("struct with optional field") {
    val json =
      """
        |{
        |  "schema": {
        |    "type": "struct",
        |    "fields": [
        |      {
        |        "field": "optional",
        |        "type": "string",
        |        "optional": true
        |      },
        |      {
        |        "field": "required",
        |        "type": "string"
        |      }
        |    ]
        |  },
        |  "payload": {
        |    "required": "required"
        |  }
        |}""".stripMargin

    val schemaAndValue = converter.toConnectData("topic", json.getBytes)
    val struct         = ValueConverter.apply(schemaAndValue.value()).getOrElse(fail("Should convert the map"))
    struct.schema().fields().asScala.map(_.name()).sorted shouldBe List("optional", "required").sorted

    struct.schema().field("optional").schema() shouldBe Schema.OPTIONAL_STRING_SCHEMA
    struct.schema().field("required").schema() shouldBe Schema.STRING_SCHEMA

    struct.get("optional") shouldBe null
    struct.get("required") shouldBe "required"
  }

  test("convert struct with decimalE") {
    val json =
      """
        |{
        |  "schema": {
        |    "type": "struct",
        |    "fields": [
        |      {
        |        "field": "field1",
        |        "type": "boolean"
        |      },
        |      {
        |        "field": "field2",
        |        "type": "string"
        |      }
        |    ]
        |  },
        |  "payload": {
        |    "field1": true,
        |    "field2": "string"
        |  }
        |}""".stripMargin

    val schemaAndValue = converter.toConnectData("topic", json.getBytes)
    val struct         = ValueConverter.apply(schemaAndValue.value()).getOrElse(fail("Should convert the map"))
    struct.schema().fields().asScala.map(_.name()).sorted shouldBe List("field1", "field2").sorted

    struct.schema().field("field1").schema() shouldBe Schema.BOOLEAN_SCHEMA
    struct.schema().field("field2").schema() shouldBe Schema.STRING_SCHEMA

    struct.get("field1") shouldBe true
    struct.get("field2") shouldBe "string"
  }
}