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

package com.celonis.kafka.connect.ems.conversion
import com.celonis.kafka.connect.ems.conversion.SchemaExtensions._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SchemaLessJsonValueConverterTest extends AnyFunSuite with Matchers {
  private val converter = new org.apache.kafka.connect.json.JsonConverter()
  converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                          "converter.type"                          -> "value",
                          "schemas.enable"                          -> "false",
  ).asJava)

  test("empty schemaless JSON returns invalid input") {
    val json           = """{}"""
    val schemaAndValue = converter.toConnectData("topic", json.getBytes)

    convert(schemaAndValue.value()).left.toOption.get shouldBe a[InvalidInputException]
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

    val record = convert(schemaAndValue.value())
      .getOrElse(fail("Should convert the map"))

    // Jackson transforming the json to Map the fields order is not retained
    record.getSchema.getFields.asScala.map(_.name()).toList.sorted shouldBe List("idType",
                                                                                 "colorDepth",
                                                                                 "threshold",
                                                                                 "evars",
                                                                                 "exclude",
                                                                                 "cars",
                                                                                 "nums",
    ).sorted

    record.getSchema.getField("idType").schema() shouldBe SchemaBuilder.builder().longType().asNullable
    record.getSchema.getField("colorDepth").schema() shouldBe SchemaBuilder.builder().stringType().asNullable

    record.getSchema.getField("threshold").schema() shouldBe SchemaBuilder.builder().doubleType().asNullable

    // record.getSchema.getField("exclude").schema().isNullable shouldBe true
    record.getSchema.getField("exclude").schema().getType shouldBe Schema.Type.RECORD

    record.getSchema.getField("evars").schema().getType shouldBe Schema.Type.RECORD
    // record.getSchema.getField("evars").schema().isNullable shouldBe true

    val evarsSchema = record.getSchema.getField("evars").schema()
    evarsSchema.getFields.asScala.map(_.name()).toList shouldBe List("evarsa")

    val evarsInner = evarsSchema.getField("evarsa").schema()
    evarsInner.isRecord shouldBe true
    evarsInner.getFields.asScala.map(_.name()).toList.sorted shouldBe List("eVar1", "eVar2").sorted
    evarsInner.getField("eVar1").schema() shouldBe SchemaBuilder.builder().stringType().asNullable
    evarsInner.getField("eVar2").schema() shouldBe SchemaBuilder.builder().longType().asNullable

    val exclude = record.getSchema.getField("exclude").schema()
    exclude.isRecord shouldBe true
//    record.getSchema.getField("exclude").schema().isNullable shouldBe true
    exclude.getFields.asScala.map(_.name()).toList.sorted shouldBe List("id", "value").sorted
    exclude.getField("id").schema() shouldBe SchemaBuilder.builder().longType().asNullable
    exclude.getField("value").schema() shouldBe SchemaBuilder.builder().booleanType().asNullable

    record.get("idType") shouldBe 3L
    record.get("colorDepth") shouldBe ""
    record.get("threshold") shouldBe 45.77d

    val evarsStruct = record.get("evars").asInstanceOf[GenericRecord].get("evarsa").asInstanceOf[GenericRecord]
    evarsStruct.get("eVar1") shouldBe "Tue Aug 27 2019 12:08:10"
    evarsStruct.get("eVar2") shouldBe 156692207943934897L

    val excludeStruct = record.get("exclude").asInstanceOf[GenericRecord]
    excludeStruct.get("id") shouldBe 0L
    excludeStruct.get("value") shouldBe false

    val carsSchema = record.getSchema.getField("cars").schema()
    carsSchema shouldBe SchemaBuilder.builder().stringType().asNullable
    record.get("cars") shouldBe """["Ford","BMW","Fiat"]"""

    val numsSchema = record.getSchema.getField("nums").schema()
    numsSchema shouldBe SchemaBuilder.builder().stringType().asNullable
    record.get("nums") shouldBe "[1,3,4]"
  }

  private val inference = new InferSchemaAndNormaliseValue(false);

  private def convert(value: Any): Either[Throwable, GenericRecord] =
    inference(value).flatMap(valueAndSchema =>
      DataConverter(valueAndSchema.normalisedValue),
    )

  implicit class SchemOps(schema: Schema) {
    def asNullable: Schema = SchemaBuilder.builder().unionOf().nullType().and().`type`(schema).endUnion()
  }
}
