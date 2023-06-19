package com.celonis.kafka.connect.ems.storage

import org.apache.avro.Schema
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.{ SchemaBuilder => ConnectSchemaBuilder }

case class ValueAndSchemas(
  name:                 String,
  avroValue:            Any,
  connectValue:         Any,
  avroSchema:           Schema,
  connectSchemaBuilder: ConnectSchemaBuilder,
  parquetSchema:        String,
) {
  lazy val connectSchema:         ConnectSchema = connectSchemaBuilder.schema()
  lazy val optionalConnectSchema: ConnectSchema = connectSchemaBuilder.optional().schema()
  lazy val optionalParquetSchema: String        = "optional " + parquetSchema
}
