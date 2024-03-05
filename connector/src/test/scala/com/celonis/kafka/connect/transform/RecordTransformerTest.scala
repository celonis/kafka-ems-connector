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

package com.celonis.kafka.connect.transform

import cats.effect.unsafe.implicits._
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.celonis.kafka.connect.transform.fields.FieldInserter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class RecordTransformerTest extends AnyFunSuite with Matchers {

  test("evolves target schema and aligns sunk record value to it") {
    val value1 = Map[String, Any](
      "a_string" -> "hello",
      "an_int"   -> 1,
      "a_float"  -> 1.5,
    ).asJava

    val value2 = Map[String, Any](
      "a_string" -> "hello again",
      // other fields omitted
    ).asJava

    val transformer =
      RecordTransformer.fromConfig(
        "mySink",
        PreConversionConfig(false),
        Some(FlattenerConfig(discardCollections = true, jsonBlobChunks = None)),
        Nil,
        None,
        allowNullsAsPks = false,
        FieldInserter.embeddedKafkaMetadata(doInsert = true, None),
      )

    val record1 = sinkRecord(value1)
    transformer.transform(record1).unsafeRunSync()
    val record2 = sinkRecord(value2)

    val genericRecord = transformer.transform(record2).unsafeRunSync()

    genericRecord.get("a_string") shouldEqual "hello again"
    genericRecord.get("an_int") shouldEqual null
    genericRecord.get("a_float") shouldEqual null
  }

  test("resets schema when schema evolution fails due to a type error") {
    val value1 = Map[String, Any](
      "some_field"       -> "hello",
      "some_other_field" -> "hello",
    ).asJava

    val value2 = Map[String, Any](
      "some_field" -> 22, // field type has changed!
    ).asJava

    val transformer =
      RecordTransformer.fromConfig(
        "mySink",
        PreConversionConfig(false),
        Some(FlattenerConfig(discardCollections = true, jsonBlobChunks = None)),
        Nil,
        None,
        allowNullsAsPks = false,
        FieldInserter.embeddedKafkaMetadata(doInsert = true, None),
      )

    val record1        = sinkRecord(value1)
    val genericRecord1 = transformer.transform(record1).unsafeRunSync()

    genericRecord1.hasField("some_field") shouldBe true
    genericRecord1.hasField("some_other_field") shouldBe true

    val record2        = sinkRecord(value2)
    val genericRecord2 = transformer.transform(record2).unsafeRunSync()

    genericRecord2.get("some_field") shouldEqual 22
    genericRecord2.hasField("some_other_field") shouldBe false
  }

  test("With Chunking enabled, heterogeneous arrays are handled properly") {
    val value = Map(
      "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
    ).asJava

    val record        = sinkRecord(value)
    val genericRecord = chunkTransform(record, 2, 20)

    genericRecord.get("payload_chunk1") shouldBe "{\"heterogeneous_arra"
    genericRecord.get("payload_chunk2") shouldBe "y\":[\"a\",1,true]}"
  }

  test("With Chunking enabled, JSON with empty keys is handled properly") {
    val value = Map(
      "12345456789012345456789" -> "x",
      ""                        -> "y",
    ).asJava

    val record        = sinkRecord(value)
    val genericRecord = chunkTransform(record, 2, 20)

    genericRecord.get("payload_chunk1") shouldBe "{\"123454567890123454"
    genericRecord.get("payload_chunk2") shouldBe "56789\":\"x\",\"\":\"y\"}"
  }

  test("With decimal conversion enabled, big decimals are converted into doubles") {
    val aBigDecimal = java.math.BigDecimal.valueOf(0.12345)
    val nestedSchema = SchemaBuilder.struct()
      .field("nested_decimal", Decimal.schema(5))
      .field("nested_float32", SchemaBuilder.float32().schema()).build()

    val schema = SchemaBuilder.struct()
      .field("nested", nestedSchema)
      .field("a_decimal", Decimal.schema(5))
      .field("an_optional_decimal", Decimal.builder(5).optional().schema())
      .field("another_optional_decimal", Decimal.builder(5).optional().schema())

    val nestedStruct = new Struct(nestedSchema)
    nestedStruct.put("nested_decimal", aBigDecimal)
    nestedStruct.put("nested_float32", 1.45f)

    val struct = new Struct(schema)
    struct.put("nested", nestedStruct)
    struct.put("a_decimal", aBigDecimal)
    struct.put("an_optional_decimal", aBigDecimal)
    struct.put("another_optional_decimal", null)

    val record        = sinkRecord(struct, schema)
    val genericRecord = decimalConversionWithNoFlattening(record)

    genericRecord.get("nested").asInstanceOf[Record].get("nested_decimal") shouldBe aBigDecimal.doubleValue()
    genericRecord.get("nested").asInstanceOf[Record].get("nested_float32") shouldBe 1.45f
    genericRecord.get("a_decimal") shouldBe aBigDecimal.doubleValue()
    genericRecord.get("an_optional_decimal") shouldBe aBigDecimal.doubleValue()
    genericRecord.get("another_optional_decimal") shouldBe null
  }

  test("With Chunking disabled, heterogeneous arrays prevent flattening") {
    val value = Map(
      "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
    ).asJava
    val record = sinkRecord(value)
    flattenTransform(record)
    ()
  }

  test("With Chunking disabled, heterogeneous arrays prevents flattening, even with discardCollection enabled") {
    val value = Map(
      "foo"                 -> "bar",
      "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
    ).asJava

    val record = sinkRecord(value)
    flattenTransform(record)
    ()
  }

  private def chunkTransform(record: SinkRecord, maxChunks: Int, chunkSize: Int): GenericRecord = {
    val flattenerConfig = Some(FlattenerConfig(discardCollections = false, Some(JsonBlobChunks(maxChunks, chunkSize))))
    val transformer =
      RecordTransformer.fromConfig("mySink",
                                   PreConversionConfig(false),
                                   flattenerConfig,
                                   Nil,
                                   None,
                                   false,
                                   FieldInserter.noop,
      )
    transformer.transform(record).unsafeRunSync()
  }

  private def flattenTransform(record: SinkRecord, discardCollections: Boolean = false): GenericRecord = {
    val flattenerConfig = Some(FlattenerConfig(discardCollections = discardCollections, None))
    val transformer =
      RecordTransformer.fromConfig("mySink",
                                   PreConversionConfig(false),
                                   flattenerConfig,
                                   Nil,
                                   None,
                                   false,
                                   FieldInserter.noop,
      )
    transformer.transform(record).unsafeRunSync()
  }

  private def decimalConversionWithNoFlattening(record: SinkRecord): GenericRecord = {
    val transformer =
      RecordTransformer.fromConfig("mySink", PreConversionConfig(true), None, Nil, None, false, FieldInserter.noop)
    transformer.transform(record).unsafeRunSync()
  }

  private def sinkRecord(value: Any, schema: Schema = null): SinkRecord =
    new SinkRecord("topic", 0, null, "aKey", schema, value, 0)
}
