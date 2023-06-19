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

package com.celonis.kafka.connect.ems.storage

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.{ SchemaBuilder => ConnectSchemaBuilder }
import org.apache.kafka.connect.sink.SinkRecord

import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.util.Random
import java.util.UUID

trait SampleData {
  val simpleSchemaV1: Schema = SchemaBuilder.record("record")
    .fields()
    .name("id").`type`(SchemaBuilder.builder().stringType()).noDefault()
    .name("int_field").`type`(SchemaBuilder.builder().intType()).noDefault()
    .name("long_field").`type`(SchemaBuilder.builder().longType()).noDefault()
    .endRecord()

  val simpleSchemaV2: Schema = SchemaBuilder.record("record")
    .fields()
    .name("id").`type`(SchemaBuilder.builder().stringType()).noDefault()
    .name("int_field").`type`(SchemaBuilder.builder().intType()).noDefault()
    .name("long_field").`type`(SchemaBuilder.builder().longType()).noDefault()
    .name("boolean_field").`type`(SchemaBuilder.builder().booleanType()).noDefault()
    .endRecord()

  def buildSimpleStruct(): GenericData.Record = {
    val rand   = new Random(System.currentTimeMillis())
    val record = new Record(simpleSchemaV1)
    record.put("id", UUID.randomUUID().toString)
    record.put("int_field", rand.nextInt())
    record.put("long_field", rand.nextLong())
    record
  }

  val userSchema: ConnectSchemaBuilder = ConnectSchemaBuilder.struct()
    .field("name", ConnectSchemaBuilder.string().required().build())
    .field("title", ConnectSchemaBuilder.string().optional().build())
    .field("salary", ConnectSchemaBuilder.float64().optional().build())

  val streetSchema = ConnectSchemaBuilder.struct()
    .field("name", ConnectSchemaBuilder.string().build())
    .field("number", ConnectSchemaBuilder.int32().build())
    .optional()
    .build()

  val nestedUserSchema: ConnectSchema =
    userSchema.field(
      "street",
      streetSchema,
    ).build()

  private val aLocalDate  = LocalDate.of(2023, 6, 1)
  private val aLocalTime  = LocalTime.ofNanoOfDay(1_000_000 * 123)
  private val anInstant   = Instant.ofEpochMilli(1686990713123L)
  private val aUUID       = UUID.randomUUID()
  private val aBigDecimal = new java.math.BigDecimal(java.math.BigInteger.valueOf(123456789), 5)

  /** Collect some expectations for values and schemas between AVRO, Connect and Parquet formats
    */
  val primitiveValuesAndSchemas: List[ValueAndSchemas] = List(
    ValueAndSchemas(
      "an_int8",
      Byte.MaxValue,
      Byte.MaxValue,
      Byte.MaxValue,
      SchemaBuilder.builder().intType(),
      ConnectSchemaBuilder.int8(),
      "int32 an_int8",
    ),
    ValueAndSchemas(
      "an_int16",
      Short.MaxValue,
      Short.MaxValue,
      Short.MaxValue,
      SchemaBuilder.builder().intType(),
      ConnectSchemaBuilder.int16(),
      "int32 an_int16",
    ),
    ValueAndSchemas(
      "an_int32",
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      SchemaBuilder.builder().intType(),
      ConnectSchemaBuilder.int32(),
      "int32 an_int32",
    ),
    ValueAndSchemas(
      "an_int64",
      Long.MaxValue,
      Long.MaxValue,
      Long.MaxValue,
      SchemaBuilder.builder().longType(),
      ConnectSchemaBuilder.int64(),
      "int64 an_int64",
    ),
    ValueAndSchemas(
      name                 = "a_float32",
      avroValue            = 0.1f,
      connectValue         = 0.1f,
      parquetValue         = 0.1f,
      avroSchema           = SchemaBuilder.builder().floatType(),
      connectSchemaBuilder = ConnectSchemaBuilder.float32(),
      parquetSchema        = "float a_float32",
    ),
    ValueAndSchemas(
      name                 = "a_float64",
      avroValue            = 123456789.123456789,
      connectValue         = 123456789.123456789,
      parquetValue         = 123456789.123456789,
      avroSchema           = SchemaBuilder.builder().doubleType(),
      connectSchemaBuilder = ConnectSchemaBuilder.float64(),
      parquetSchema        = "double a_float64",
    ),
    ValueAndSchemas(
      name                 = "a_bool",
      avroValue            = true,
      connectValue         = true,
      parquetValue         = true,
      avroSchema           = SchemaBuilder.builder().booleanType(),
      connectSchemaBuilder = ConnectSchemaBuilder.bool(),
      parquetSchema        = "boolean a_bool",
    ),
    ValueAndSchemas(
      name                 = "a_string",
      avroValue            = "abc",
      connectValue         = "abc",
      parquetValue         = new Utf8("abc"),
      avroSchema           = SchemaBuilder.builder().stringType(),
      connectSchemaBuilder = ConnectSchemaBuilder.string(),
      parquetSchema        = "binary a_string (STRING)",
    ),
    ValueAndSchemas(
      name                 = "some_bytes",
      avroValue            = ByteBuffer.wrap("abc".getBytes),
      connectValue         = ByteBuffer.wrap("abc".getBytes),
      parquetValue         = ByteBuffer.wrap("abc".getBytes),
      avroSchema           = SchemaBuilder.builder().bytesType(),
      connectSchemaBuilder = ConnectSchemaBuilder.bytes(),
      parquetSchema        = "binary some_bytes",
    ),
    // Logical types
    ValueAndSchemas(
      name                 = "a_date",
      avroValue            = aLocalDate,
      connectValue         = java.util.Date.from(aLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant),
      parquetValue         = aLocalDate,
      avroSchema           = SchemaBuilder.builder().intType().withLogicalType(LogicalTypes.date()),
      connectSchemaBuilder = org.apache.kafka.connect.data.Date.builder(),
      parquetSchema        = "int32 a_date (DATE)",
    ),
    ValueAndSchemas(
      "a_timeMillis",
      aLocalTime,
      java.util.Date.from(Instant.ofEpochMilli(aLocalTime.getNano.toLong / 1_000_000)),
      aLocalTime,
      SchemaBuilder.builder().intType().withLogicalType(LogicalTypes.timeMillis()),
      org.apache.kafka.connect.data.Time.builder(),
      "int32 a_timeMillis (TIME(MILLIS,true))",
    ),
    ValueAndSchemas(
      name                 = "a_timestampMillis",
      avroValue            = anInstant,
      connectValue         = java.util.Date.from(anInstant),
      parquetValue         = anInstant,
      avroSchema           = SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timestampMillis()),
      connectSchemaBuilder = org.apache.kafka.connect.data.Timestamp.builder(),
      parquetSchema        = "int64 a_timestampMillis (TIMESTAMP(MILLIS,true))",
    ),
    ValueAndSchemas(
      name                 = "timeMicros",
      avroValue            = LocalTime.ofNanoOfDay(123_001_000),
      connectValue         = 123_001L,
      parquetValue         = 123_001L,
      avroSchema           = SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timeMicros()),
      connectSchemaBuilder = org.apache.kafka.connect.data.SchemaBuilder.int64(),
      parquetSchema        = "int64 timeMicros",
    ),
    ValueAndSchemas(
      name                 = "a_timestampMicros",
      avroValue            = Instant.ofEpochMilli(1686990713123L).plusNanos(1000),
      connectValue         = 1686990713123001L,
      parquetValue         = 1686990713123001L,
      avroSchema           = SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timestampMicros()),
      connectSchemaBuilder = org.apache.kafka.connect.data.SchemaBuilder.int64(),
      parquetSchema        = "int64 a_timestampMicros",
    ),
    ValueAndSchemas(
      name                 = "a_decimal",
      avroValue            = aBigDecimal,
      connectValue         = aBigDecimal,
      parquetValue         = aBigDecimal,
      avroSchema           = SchemaBuilder.builder().bytesType().withLogicalType(LogicalTypes.decimal(9, 5)),
      connectSchemaBuilder = org.apache.kafka.connect.data.Decimal.builder(5),
      parquetSchema        = "binary a_decimal (DECIMAL(9,5))",
    ),
    ValueAndSchemas(
      name                 = "a_uuid",
      avroValue            = aUUID,
      connectValue         = aUUID.toString,
      parquetValue         = new Utf8(aUUID.toString),
      avroSchema           = SchemaBuilder.builder().stringType().withLogicalType(LogicalTypes.uuid()),
      connectSchemaBuilder = org.apache.kafka.connect.data.SchemaBuilder.string(),
      parquetSchema        = "binary a_uuid (STRING)",
    ),
  )

  implicit class AvroSchemaOps(schema: Schema) {
    def withLogicalType(logicalType: LogicalType): Schema = logicalType.addToSchema(schema)
  }

  def buildUserStruct(name: String, title: String, salary: Double): Struct =
    new Struct(userSchema.build()).put("name", name).put("title", title).put("salary", salary)

  def toSinkRecord(topic: String, value: Struct, k: Int): SinkRecord =
    new SinkRecord(topic, 1, null, null, value.schema(), value, k.toLong)

}

object SampleData extends SampleData
