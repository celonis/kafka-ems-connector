/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion
import cats.syntax.either._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import SchemaExtensions._

class SchemaLessJsonValueConverterTest extends AnyFunSuite with Matchers {
  private val converter = new org.apache.kafka.connect.json.JsonConverter()
  converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                          "converter.type"                          -> "value",
                          "schemas.enable"                          -> "false",
  ).asJava)

  test("empty schemaless JSON returns invalid input") {
    val json           = """{}"""
    val schemaAndValue = converter.toConnectData("topic", json.getBytes)

    DataConverter.apply(schemaAndValue.value()) shouldBe InvalidInputException(
      s"Invalid input received. The connector has received an empty input which cannot be written to Parquet. This can happen for empty JSON objects. ",
    ).asLeft
  }

  test("convert nested schemaless JSON") {
    val json =
      """
        |{
        |  "idType": 3,
        |  "colorDepth": "",
        |  "threshold" : 45.77,
        |  "evars": {
        |    "evarsa": {
        |      "eVar1": "Tue Aug 27 2019 12:08:10",
        |      "eVar2": 156692207943934897
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

    val schemaAndValue = converter.toConnectData("topic", json.getBytes)

    val record = DataConverter.apply(schemaAndValue.value())
      .getOrElse(fail("Should convert the map"))

    //Jackson transforming the json to Map the fields order is not retained
    record.getSchema.getFields.asScala.map(_.name()).toList.sorted shouldBe List("idType",
                                                                                 "colorDepth",
                                                                                 "threshold",
                                                                                 "evars",
                                                                                 "exclude",
                                                                                 "cars",
                                                                                 "nums",
    ).sorted

    record.getSchema.getField("idType").schema() shouldBe SchemaBuilder.builder().nullable().longType()
    record.getSchema.getField("colorDepth").schema() shouldBe SchemaBuilder.builder().nullable().stringType()

    record.getSchema.getField("threshold").schema() shouldBe SchemaBuilder.builder().nullable().doubleType()

    record.getSchema.getField("exclude").schema().isNullable shouldBe true
    record.getSchema.getField("exclude").schema().getTypes.asScala.exists(_.getType == Schema.Type.RECORD) shouldBe true

    record.getSchema.getField("evars").schema().getTypes.asScala.exists(_.getType == Schema.Type.RECORD) shouldBe true
    record.getSchema.getField("evars").schema().isNullable shouldBe true

    val evarsSchema = record.getSchema.getField("evars").schema().nonNullableSchema.get
    evarsSchema.getFields.asScala.map(_.name()).toList shouldBe List("evarsa")

    val evarsInner = evarsSchema.getField("evarsa").schema().nonNullableSchema.get
    evarsInner.isRecord shouldBe true
    evarsInner.getFields.asScala.map(_.name()).toList.sorted shouldBe List("eVar1", "eVar2").sorted
    evarsInner.getField("eVar1").schema() shouldBe SchemaBuilder.builder().nullable().stringType()
    evarsInner.getField("eVar2").schema() shouldBe SchemaBuilder.builder().nullable().longType()

    val exclude = record.getSchema.getField("exclude").schema().nonNullableSchema.get
    exclude.isRecord shouldBe true
    record.getSchema.getField("exclude").schema().isNullable shouldBe true
    exclude.getFields.asScala.map(_.name()).toList.sorted shouldBe List("id", "value").sorted
    exclude.getField("id").schema() shouldBe SchemaBuilder.builder().nullable().longType()
    exclude.getField("value").schema() shouldBe SchemaBuilder.builder().nullable().booleanType()

    record.get("idType") shouldBe 3L
    record.get("colorDepth") shouldBe ""
    record.get("threshold") shouldBe 45.77d

    val evarsStruct = record.get("evars").asInstanceOf[GenericRecord].get("evarsa").asInstanceOf[GenericRecord]
    evarsStruct.get("eVar1") shouldBe "Tue Aug 27 2019 12:08:10"
    evarsStruct.get("eVar2") shouldBe 156692207943934897L

    val excludeStruct = record.get("exclude").asInstanceOf[GenericRecord]
    excludeStruct.get("id") shouldBe 0L
    excludeStruct.get("value") shouldBe false

    val carsSchema = record.getSchema.getField("cars").schema().nonNullableSchema.get
    carsSchema.getType shouldBe Schema.Type.ARRAY
    carsSchema.getElementType shouldBe SchemaBuilder.builder().stringType()
    record.get("cars").toString shouldBe "[Ford, BMW, Fiat]"

    val numsSchema = record.getSchema.getField("nums").schema().nonNullableSchema.get
    numsSchema.getType shouldBe Schema.Type.ARRAY
    numsSchema.getElementType shouldBe SchemaBuilder.builder().longType()
    record.get("nums").toString shouldBe "[1, 3, 4]"
  }
}
