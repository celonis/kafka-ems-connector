/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion

import com.celonis.kafka.connect.ems.model.BooleanSinkData
import com.celonis.kafka.connect.ems.model.ByteArraySinkData
import com.celonis.kafka.connect.ems.model.ByteSinkData
import com.celonis.kafka.connect.ems.model.DoubleSinkData
import com.celonis.kafka.connect.ems.model.FloatSinkData
import com.celonis.kafka.connect.ems.model.IntSinkData
import com.celonis.kafka.connect.ems.model.LongSinkData
import com.celonis.kafka.connect.ems.model.SinkData
import com.celonis.kafka.connect.ems.model.StringSinkData
import com.celonis.kafka.connect.ems.model.StructSinkData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

object ValueToSinkDataConverter {

  def apply(value: Any, schema: Option[Schema]): SinkData = value match {
    case boolVal:   Boolean => BooleanSinkData(boolVal)
    case stringVal: String  => StringSinkData(stringVal)
    case longVal:   Long    => LongSinkData(longVal)
    case intVal:    Int     => IntSinkData(intVal)
    case byteVal:   Byte    => ByteSinkData(byteVal)
    case doubleVal: Double  => DoubleSinkData(doubleVal)
    case floatVal:  Float   => FloatSinkData(floatVal)
    case structVal: Struct  => StructSinkData(structVal)
    //case mapVal:    Map[_, _]      => MapSinkDataConverter(mapVal, schema)
    //case mapVal:    util.Map[_, _] => MapSinkDataConverter(mapVal.asScala.toMap, schema)
    case bytesVal: Array[Byte] => ByteArraySinkData(bytesVal)
    //case arrayVal:  Array[_]       => ArraySinkDataConverter(arrayVal, schema)
    //case listVal:   util.List[_]   => ArraySinkDataConverter(listVal.toArray, schema)
    //case null     => NullSinkData(schema)
    case otherVal => throw new ConnectException(s"Unsupported record $otherVal:${otherVal.getClass.getCanonicalName}")
  }
}
/*

object ArraySinkDataConverter {
  def apply(array: Array[_], schema: Option[Schema]): SinkData =
    ArraySinkData(array.map(e => ValueToSinkDataConverter(e, None)), schema)
}

object MapSinkDataConverter {
  def apply(map: Map[_, _], schema: Option[Schema]): SinkData =
    MapSinkData(
      map.map {
        case (k: String, v) => StringSinkData(k) -> ValueToSinkDataConverter(v, None)
        case (k, _) => throw new ConnectException(
            s"Non-string map values including (${k.getClass.getCanonicalName}) are not currently supported",
          )
      },
      schema,
    )
}
 */
