package com.celonis.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import scala.jdk.CollectionConverters._
import cats.instances.list._
import cats.instances.option._
import cats.syntax.foldable._
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
  def apply(value: Any): Option[Schema] = value match {
    case _: String =>
      Some(Schema.OPTIONAL_STRING_SCHEMA)
    case _: Long =>
      Some(Schema.OPTIONAL_INT64_SCHEMA)
    case _: Int =>
      Some(Schema.OPTIONAL_INT32_SCHEMA)
    case _: Boolean =>
      Some(Schema.OPTIONAL_BOOLEAN_SCHEMA)
    case _: Float =>
      Some(Schema.OPTIONAL_FLOAT64_SCHEMA)
    case _: Double =>
      Some(Schema.OPTIONAL_FLOAT64_SCHEMA)
    case list: java.util.List[_] =>
      listSchema(list.asScala.toList)
    case innerMap: java.util.Map[_, _] =>
      mapSchema(innerMap.asScala.toMap)
    case _ =>
      None
  }

  private def mapSchema(values: Map[_, _]): Option[Schema] =
    if (values.isEmpty)
      Some(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build())
    else
      values.toList.foldM(SchemaBuilder.struct()) {
        case (b, (key, value)) =>
          SchemaInference(value).map(b.field(key.toString, _))
      }.map(_.build())

  private def listSchema(values: List[_]): Option[Schema] =
    values.traverse(SchemaInference(_)).map { valueSchemas =>
      if (valueSchemas.isEmpty || valueSchemas.toSet.size > 1)
        SchemaBuilder.array(Schema.BYTES_SCHEMA).build()
      else
        SchemaBuilder.array(valueSchemas.head).build()
    }

}
