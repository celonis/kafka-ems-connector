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
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.{ SchemaBuilder => ConnectSchemaBuilder }
import org.apache.kafka.connect.sink.SinkRecord

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

  private val aLocalDate = LocalDate.of(2023, 6, 1)
  private val aLocalTime = LocalTime.ofNanoOfDay(1_000_000 * 123)
  private val anInstant  = Instant.ofEpochMilli(1686990713123L)
  private val aUUID      = UUID.randomUUID()

  /** Java values and their respective AVRO and Connect schema
    */
  val primitiveValuesAndSchemas: List[ValueAndSchemas] = List(
    ValueAndSchemas(
      "an_int8",
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
      SchemaBuilder.builder().intType(),
      ConnectSchemaBuilder.int16(),
      "int32 an_int16",
    ),
    ValueAndSchemas(
      "an_int32",
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
      SchemaBuilder.builder().intType(),
      ConnectSchemaBuilder.int64(),
      "int64 an_int64",
    ),
    ValueAndSchemas(
      "a_float32",
      0.1f,
      0.1f,
      SchemaBuilder.builder().floatType(),
      ConnectSchemaBuilder.float32(),
      "float a_float32",
    ),
    ValueAndSchemas(
      "a_float64",
      123456789.123456789,
      123456789.123456789,
      SchemaBuilder.builder().doubleType(),
      ConnectSchemaBuilder.float64(),
      "double a_float64",
    ),
    ValueAndSchemas(
      "a_bool",
      true,
      true,
      SchemaBuilder.builder().booleanType(),
      ConnectSchemaBuilder.bool(),
      "booelan a_bool",
    ),
    ValueAndSchemas(
      "a_string",
      "abc",
      "abc",
      SchemaBuilder.builder().stringType(),
      ConnectSchemaBuilder.string(),
      "string a_string",
    ),
    ValueAndSchemas(
      "some_bytes",
      "abc".getBytes,
      "abc".getBytes,
      SchemaBuilder.builder().stringType(),
      ConnectSchemaBuilder.bytes(),
      "binary some_bytes",
    ),
    // Logical types
    ValueAndSchemas(
      "a_date",
      aLocalDate,
      java.util.Date.from(aLocalDate.atStartOfDay(ZoneId.systemDefault()).toInstant),
      SchemaBuilder.builder().intType().withLogicalType(LogicalTypes.date()),
      org.apache.kafka.connect.data.Date.builder(),
      "int32 a_date (DATE)",
    ),
    ValueAndSchemas(
      "a_timeMillis",
      aLocalTime,
      java.util.Date.from(Instant.ofEpochMilli(aLocalTime.getNano.toLong / 1_000_000)),
      SchemaBuilder.builder().intType().withLogicalType(LogicalTypes.timeMillis()),
      org.apache.kafka.connect.data.Time.builder(),
      "int32 a_timeMillis (TIME(MILLIS,true))",
    ),
    ValueAndSchemas(
      "a_timestampMillis",
      Instant.ofEpochMilli(1686990713123L),
      java.util.Date.from(Instant.ofEpochMilli(anInstant.getNano.toLong / 1_000_000)),
      SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timestampMillis()),
      org.apache.kafka.connect.data.Timestamp.builder(),
      "int64 a_timestampMillis (TIMESTAMP(MILLIS,true))",
    ),
    ValueAndSchemas(
      "timeMicros",
      LocalTime.ofNanoOfDay(1_000_000 * 123 + 1000),
      1_000_000L * 123 + 1000,
      SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timeMicros()),
      org.apache.kafka.connect.data.SchemaBuilder.int64(),
      "int64 timeMicros",
    ),
    ValueAndSchemas(
      "a_timestampMicros",
      Instant.ofEpochMilli(1686990713123L).plusNanos(1000),
      1686990713123001L,
      SchemaBuilder.builder().longType().withLogicalType(LogicalTypes.timestampMicros()),
      org.apache.kafka.connect.data.SchemaBuilder.int64(),
      "int64 a_timestampMicros",
    ),
    ValueAndSchemas(
      "a_decimal",
      new java.math.BigDecimal(java.math.BigInteger.valueOf(123456789), 5),
      new java.math.BigDecimal(java.math.BigInteger.valueOf(123456789), 5),
      SchemaBuilder.builder().bytesType().withLogicalType(LogicalTypes.decimal(9, 5)),
      org.apache.kafka.connect.data.Decimal.builder(5),
      "binary a_decimal (DECIMAL(9,5))",
    ),
    ValueAndSchemas(
      "a_uuid",
      aUUID,
      aUUID.toString,
      SchemaBuilder.builder().stringType().withLogicalType(LogicalTypes.uuid()),
      org.apache.kafka.connect.data.SchemaBuilder.string(),
      "binary a_uuid (STRING)",
    ),
  ).take(1)

  implicit class AvroSchemaOps(schema: Schema) {
    def withLogicalType(logicalType: LogicalType): Schema = logicalType.addToSchema(schema)
  }

  def buildUserStruct(name: String, title: String, salary: Double): Struct =
    new Struct(userSchema.build()).put("name", name).put("title", title).put("salary", salary)

  def toSinkRecord(topic: String, value: Struct, k: Int): SinkRecord =
    new SinkRecord(topic, 1, null, null, value.schema(), value, k.toLong)

}
