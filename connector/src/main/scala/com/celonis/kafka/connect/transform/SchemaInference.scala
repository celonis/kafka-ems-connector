package com.celonis.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.traverse._

object SchemaInference {

  /** Tries to infer a non-flat Kafka connect schema for a value.
    *
    * This is expected to be called with a top-level java.util.Map (i.e. the Kafka Connect
    * internal representation for schemaless JSON objects).
    *
    * @param value
    * @return
    */
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
      val inferredValues = values.asScala.toList.traverse {
        case (key, value) => SchemaInference(value).map(key.toString -> _)
      }
      inferredValues.map(values => toStruct(values.toMap))
    }

  private def listSchema(values: java.util.List[_]): Option[ValueAndSchema] =
    values.asScala.toList.traverse(SchemaInference.apply).map { results =>
      if (results.isEmpty || results.map(_.schema).toSet.size > 1)
        ValueAndSchema(values, SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
      else
        ValueAndSchema(results.map(_.normalisedValue).asJava, SchemaBuilder.array(results.head.schema).build())
    }

  private def toStruct(values: Map[String, ValueAndSchema]): ValueAndSchema = {
    val schema = values.foldLeft(SchemaBuilder.struct()) {
      case (b, (key, result)) => b.field(key, result.schema)
    }.build()
    val struct = values.foldLeft(new Struct(schema)) {
      case (struct, (key, result)) => struct.put(key, result.normalisedValue)
    }
    ValueAndSchema(struct, schema)
  }

  case class ValueAndSchema(normalisedValue: Any, schema: Schema)
}
