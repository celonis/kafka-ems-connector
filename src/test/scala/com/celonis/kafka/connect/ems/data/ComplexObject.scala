/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.data
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

case class ComplexObject(
  int8:    Byte,
  int16:   Short,
  int32:   Int,
  int64:   Long,
  float32: Float,
  float64: Double,
  b:       Boolean,
  s:       String,
  a:       Array[Byte],
  l:       List[String],
  m:       Map[String, Int],
  d:       Map[String, Int],
)
object ComplexObject {
  implicit val codec: Codec[ComplexObject] = deriveCodec
}
