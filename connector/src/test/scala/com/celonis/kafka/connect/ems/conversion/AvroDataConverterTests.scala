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
import cats.data.NonEmptySeq
import com.celonis.kafka.connect.ems.storage.formats.ListExploder
//import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{ Schema => AvroSchema }
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

class AvroDataConverterTests extends AnyFunSuite with Matchers {
  test("convert complex structure") {
    val schema: Schema = SchemaBuilder
      .struct
      .field("int8", SchemaBuilder.int8.defaultValue(2.toByte).build)
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build)
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build)
      .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build)
      .build

    val struct = new Struct(schema)
      .put("int8", 8.toByte)
      .put("int16", 8.toShort)
      .put("int32", 8)
      .put("int64", 8L)
      .put("float32", 8.8f)
      .put("float64", 8.8)
      .put("boolean", true)
      .put("string", "foo")
      .put("bytes", ByteBuffer.wrap("foo".getBytes))
      .put("array", util.Arrays.asList("a", "b", "c"))
      .put("map", Collections.singletonMap("field", 1))
      .put("mapNonStringKeys", Collections.singletonMap(1, 1))

    val gr = DataConverter(struct).getOrElse(fail("should convert"))
    gr.getSchema.getField("int8").schema().getType shouldBe AvroSchema.Type.INT
    gr.get("int8") shouldBe 8.toByte

    gr.getSchema.getField("int16").schema().getType shouldBe AvroSchema.Type.INT
    gr.get("int16") shouldBe 8.toShort

    gr.getSchema.getField("int32").schema().getType shouldBe AvroSchema.Type.INT
    gr.get("int32") shouldBe 8

    gr.getSchema.getField("int64").schema().getType shouldBe AvroSchema.Type.LONG
    gr.get("int64") shouldBe 8L

    gr.getSchema.getField("float32").schema().getType shouldBe AvroSchema.Type.FLOAT
    gr.get("float32") shouldBe 8.8f

    gr.getSchema.getField("float64").schema().getType shouldBe AvroSchema.Type.DOUBLE
    gr.get("float64") shouldBe 8.8

    gr.getSchema.getField("boolean").schema().getType shouldBe AvroSchema.Type.BOOLEAN
    gr.get("boolean") shouldBe true

    gr.getSchema.getField("array").schema().getType shouldBe AvroSchema.Type.ARRAY
    gr.get("array").asInstanceOf[util.ArrayList[_]].asScala shouldBe List("a", "b", "c")

    gr.getSchema.getField("map").schema().getType shouldBe AvroSchema.Type.MAP
    gr.get("map").asInstanceOf[util.Map[_, _]].asScala shouldBe Map("field" -> 1)

    gr.getSchema.getField("mapNonStringKeys").schema().getType shouldBe AvroSchema.Type.ARRAY
    gr.getSchema.getField("mapNonStringKeys").schema().getElementType.getType shouldBe AvroSchema.Type.RECORD

    val item = gr.get("mapNonStringKeys").asInstanceOf[util.ArrayList[_]].asScala.head.asInstanceOf[GenericRecord]
    item.get("key") shouldBe 1
    item.get("value") shouldBe 1
  }

  test("explode structure") {

    val simpleSchema: Schema = SchemaBuilder.struct()
      .name("simpleSchema")
      .field("id", SchemaBuilder.string())
      .field("int_field", SchemaBuilder.int32())
      .field("long_field", SchemaBuilder.int64())

    val containerSchema: Schema = SchemaBuilder
      .struct()
      .field("messages", SchemaBuilder.array(simpleSchema))

    val itemStruct1 = new Struct(simpleSchema)
      .put("id", "beans")
      .put("int_field", 1)
      .put("long_field", 1L)

    val itemStruct2 = new Struct(simpleSchema)
      .put("id", "toast")
      .put("int_field", 2)
      .put("long_field", 2L)

    // val avroSchema = ToAvroDataConverter.convertSchema(containerSchema)

    val avroData = new AvroData(0)

    val expectedItemStruct1 = avroData.fromConnectData(simpleSchema, itemStruct1)
    val expectedItemStruct2 = avroData.fromConnectData(simpleSchema, itemStruct2)

    val exploder = new ListExploder
    val containerStruct = new Struct(containerSchema)
      .put("messages", List(itemStruct1, itemStruct2).asJava)

    val exploded = DataConverter(containerStruct) match {
      case Left(err)     => fail(err)
      case Right(record) => exploder.explode(record)
    }
    exploded should be(NonEmptySeq.of(expectedItemStruct1, expectedItemStruct2))
  }

}
