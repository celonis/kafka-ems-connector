/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.util.Random
import java.util.UUID

trait SampleData {
  val simpleSchema: Schema = SchemaBuilder.struct.name("record")
    .version(1)
    .field("id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .build
  def buildSimpleStruct(): Struct = {
    val rand   = new Random(System.currentTimeMillis())
    val struct = new Struct(simpleSchema)
    struct.put("id", UUID.randomUUID().toString)
    struct.put("int_field", rand.nextInt())
    struct.put("long_field", rand.nextLong())
    struct
  }
}
