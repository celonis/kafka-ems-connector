/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion

import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.data.Struct

object ToAvroDataConverter {
  private val avroDataConverter = new AvroData(100)
  def convertSchema(connectSchema: ConnectSchema): Schema =
    avroDataConverter.fromConnectSchema(connectSchema)

  def convertToGenericRecord(struct: Struct): AnyRef =
    avroDataConverter.fromConnectData(struct.schema(), struct)

}
