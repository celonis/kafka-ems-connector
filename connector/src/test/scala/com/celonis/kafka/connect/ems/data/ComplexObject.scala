/*
 * Copyright 2022 Celonis SE
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
