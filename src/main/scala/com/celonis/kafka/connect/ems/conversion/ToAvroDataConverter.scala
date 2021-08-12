/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion

import com.celonis.kafka.connect.ems.model.ArraySinkData
import com.celonis.kafka.connect.ems.model.ByteArraySinkData
import com.celonis.kafka.connect.ems.model.MapSinkData
import com.celonis.kafka.connect.ems.model.NullSinkData
import com.celonis.kafka.connect.ems.model.PrimitiveSinkData
import com.celonis.kafka.connect.ems.model.SinkData
import com.celonis.kafka.connect.ems.model.StructSinkData

import java.nio.ByteBuffer
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import scala.jdk.CollectionConverters._

object ToAvroDataConverter {
  def convertSchema(connectSchema: ConnectSchema): Schema = {
    val avroDataConverter = new AvroData(1)
    avroDataConverter.fromConnectSchema(connectSchema)
  }

  def convertToGenericRecord(sinkData: SinkData): AnyRef =
    sinkData match {
      case StructSinkData(structVal) =>
        val avroDataConverter = new AvroData(1)
        avroDataConverter.fromConnectData(structVal.schema(), structVal)
      case MapSinkData(map, _)      => convertMap(map)
      case ArraySinkData(array, _)  => convertArray(array)
      case ByteArraySinkData(array) => ByteBuffer.wrap(array)
      case primitive: PrimitiveSinkData => primitive.value.asInstanceOf[AnyRef]
      case _:         NullSinkData      => null
      case other => throw new IllegalArgumentException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }

  def convertArray(array: Seq[SinkData]): java.util.List[Any] = array.map {
    case data: PrimitiveSinkData => data.value
    case StructSinkData(structVal) => structVal
    case MapSinkData(map, _)       => convertMap(map)
    case ArraySinkData(iArray, _)  => convertArray(iArray)
    case ByteArraySinkData(bArray) => ByteBuffer.wrap(bArray)
    case _                         => throw new IllegalArgumentException("Complex array writing not currently supported")
  }.asJava

  def convertMap(map: Map[SinkData, SinkData]): java.util.Map[AnyRef, AnyRef] = map.map {
    case (data, data1) => convertToGenericRecord(data) -> convertToGenericRecord(data1)
  }.asJava

}
