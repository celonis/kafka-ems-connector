/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord

import java.util.Random
import java.util.UUID

trait SampleData {
  val simpleSchema: Schema = SchemaBuilder.record("record")
    .fields()
    .name("id").`type`(SchemaBuilder.builder().stringType()).noDefault()
    .name("int_field").`type`(SchemaBuilder.builder().intType()).noDefault()
    .name("long_field").`type`(SchemaBuilder.builder().longType()).noDefault()
    .endRecord()

  def buildSimpleStruct(): GenericRecord = {
    val rand   = new Random(System.currentTimeMillis())
    val record = new Record(simpleSchema)
    record.put("id", UUID.randomUUID().toString)
    record.put("int_field", rand.nextInt())
    record.put("long_field", rand.nextLong())
    record
  }
}
