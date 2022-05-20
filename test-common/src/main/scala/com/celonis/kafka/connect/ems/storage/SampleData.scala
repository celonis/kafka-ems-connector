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

  val userSchema: ConnectSchema = ConnectSchemaBuilder.struct()
    .field("name", ConnectSchemaBuilder.string().required().build())
    .field("title", ConnectSchemaBuilder.string().optional().build())
    .field("salary", ConnectSchemaBuilder.float64().optional().build())
    .build()

  def buildUserStruct(name: String, title: String, salary: Double): Struct = {
    new Struct(userSchema).put("name", name).put("title", title).put("salary", salary)
  }

  def toSinkRecord(topic: String, user: Struct, k: Int): SinkRecord =
    new SinkRecord(topic, 1, null, null, userSchema, user, k.toLong)

}
