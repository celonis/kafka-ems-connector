/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion
import org.apache.avro.{ Schema => AvroSchema }
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._

class ToAvroDataConverterTests extends AnyFunSuite with Matchers {
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

    val sinkData = ValueConverter.apply(struct).getOrElse(fail("should convert"))
    val actual   = ToAvroDataConverter.convertToGenericRecord(sinkData)
    actual match {
      case gr: GenericRecord =>
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

      case _ => fail("Expecting a generic record")
    }
  }
}
