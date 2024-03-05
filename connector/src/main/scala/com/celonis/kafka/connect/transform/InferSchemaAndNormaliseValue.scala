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

package com.celonis.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.traverse._
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import com.celonis.kafka.connect.transform.flatten.ConnectJsonConverter

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ListMap

/** This component does multiple things:
  *   1. It infers the connect schema of the connect value 2. It normalises the value, replacing Maps (coming from json)
  *      to Structs 3. It replaces non-avro field names with avro field names (in maps only for now)
  *
  * We should split inference from normalisation, even if that will complicate the implementation
  */
final class InferSchemaAndNormaliseValue(discardCollections: Boolean) {

  /** Tries to infer a non-flat Kafka connect schema for a value.
    *
    * This is expected to be called with a top-level java.util.Map (i.e. the Kafka Connect internal representation for
    * schemaless JSON objects).
    *
    * @param value
    * @return
    */

  // TODO: Why optionals at this stage?
  def apply(value: Any): Option[ValueAndSchema] = value match {
    case _: String =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_STRING_SCHEMA))
    case _: Long =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_INT64_SCHEMA))
    case _: Int =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_INT32_SCHEMA))
    case _: Boolean =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_BOOLEAN_SCHEMA))
    case _: Float =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_FLOAT64_SCHEMA))
    case _: Double =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_FLOAT64_SCHEMA))
    case value: Struct =>
      Some(ValueAndSchema(value, value.schema()))
    case _: Array[Byte] =>
      Some(ValueAndSchema(value, Schema.OPTIONAL_BYTES_SCHEMA))
    case list: java.util.List[_] =>
      listSchema(list)
    case innerMap: java.util.Map[_, _] =>
      mapSchema(innerMap)
    case _ =>
      None
  }

  private def mapSchema(values: java.util.Map[_, _]): Option[ValueAndSchema] =
    if (values.isEmpty) // TODO test this
      Some(ValueAndSchema(values, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build()))
    else {
      val inferredValues = values.asScala.toMap.filterNot(isValueToBeDiscarded).toList.traverse {
        case (key, value) if key.toString.nonEmpty => apply(value).map(key.toString -> _)
        case _                                     => None
      }
      inferredValues.map(values => toStruct(ListMap.from(values)))
    }

  /** We discard values if they are null, and if they are lists when discardCollections is set to true
    */
  private def isValueToBeDiscarded(keyValue: (Any, Any)): Boolean = keyValue match {
    case (_, null)                                       => true // discard fields with empty value
    case (_, _: java.util.List[_]) if discardCollections => true // discard arrays when discardCollection is true
    case _                                               => false
  }

  private def listSchema(values: java.util.List[_]): Option[ValueAndSchema] = {

    val normalisedValue = new String(
      ConnectJsonConverter.converter.fromConnectData("ignored", null, values),
      StandardCharsets.UTF_8,
    )
    Some(ValueAndSchema(normalisedValue, Schema.OPTIONAL_STRING_SCHEMA))
  }

  private def toStruct(values: ListMap[String, ValueAndSchema]): ValueAndSchema = {
    val schema = values.foldLeft(SchemaBuilder.struct()) {
      case (b, (key, result)) => b.field(key, result.schema)
    }.build()
    val struct = values.foldLeft(new Struct(schema)) {
      case (struct, (key, result)) => struct.put(key, result.normalisedValue)
    }
    ValueAndSchema(struct, schema)
  }

}

object InferSchemaAndNormaliseValue {
  case class ValueAndSchema(normalisedValue: Any, schema: Schema)
}
