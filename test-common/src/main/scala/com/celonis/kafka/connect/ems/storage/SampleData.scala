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
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.{ SchemaBuilder => ConnectSchemaBuilder }
import org.apache.kafka.connect.sink.SinkRecord

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

  def buildUserStruct(name: String, title: String, salary: Double): Struct =
    new Struct(userSchema.build()).put("name", name).put("title", title).put("salary", salary)

  def toSinkRecord(topic: String, value: Struct, k: Int): SinkRecord =
    new SinkRecord(topic, 1, null, null, value.schema(), value, k.toLong)

}
