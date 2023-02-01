/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener.pathDelimiter
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

final class Flattener(discardCollections: Boolean) extends LazyLogging {

  /**
    * Flattens a Kafka Connect record value
    *
    * @param value A kafka connect record value
    * @return
    */
  def flatten(value: Any, originalSchema: Schema): Any =
    //do nothing if top-level schema is not a record{
    {
      val old =
        if (originalSchema.`type` != Schema.Type.STRUCT)
          value
        else {
          val fields = flattenFields(Vector.empty, originalSchema, value)
          structFrom(fields, schemaFrom(fields))
        }

      println(old)
      new Flattner(discardCollections).flatten(value, new SchemaFlattner(discardCollections).flatten(originalSchema))
    }

  private def flattenFields(
    path:   Seq[String],
    schema: Schema,
    value:  Any,
  ): Vector[FieldNode] =
    value match {
      case value: Struct =>
        val structFields = value.schema.fields.asScala.toVector
        structFields.flatMap(field => flattenFields(path :+ field.name(), field.schema(), value.get(field.name())))

      // Map as a struct
      case value: java.util.Map[_, _] if schema.`type`() == Schema.Type.STRUCT =>
        val structFields = schema.fields.asScala.toVector
        structFields.flatMap {
          field => flattenFields(path :+ field.name(), field.schema(), value.get(field.name()))
        }

      case _: java.util.Map[_, _] | _: java.util.List[_] =>
        discardOrJsonEncodeCollection(value, path, schema)

      case _ =>
        Vector(FieldNode(path, value, schema))
    }

  private def schemaFrom(fields: Vector[FieldNode]): Schema =
    fields.foldLeft(SchemaBuilder.struct()) { (builder, field) =>
      builder.field(field.pathAsString, withOptionalPrimitives(field.schema))
    }.build()

  private def structFrom(fields: Vector[FieldNode], flatSchema: Schema): Struct =
    fields.foldLeft(new Struct(flatSchema)) { (struct, field) =>
      val fieldName = field.pathAsString
      struct.put(fieldName, field.value)
    }

  private def withOptionalPrimitives(schema: Schema): Schema =
    schema.`type`() match {
      case tpe if tpe.isPrimitive => new SchemaBuilder(tpe).optional().build()
      case _                      => schema
    }

  private def discardOrJsonEncodeCollection(
    value:  Any,
    path:   Seq[String],
    schema: Schema,
  ): Vector[FieldNode] =
    if (discardCollections)
      Vector.empty
    else {
      val convertSchema = Some(schema).filter(_ => isCollectionOfStructs(value)).orNull
      val json = new String(
        ConnectJsonConverter.converter.fromConnectData(ConverterTopicName, convertSchema, value),
        StandardCharsets.UTF_8,
      )
      Vector(FieldNode(path, json, Schema.OPTIONAL_STRING_SCHEMA))
    }

  /**
    * Weird hack due to the fact that the connect json converter does not handle `Map`s as structs.
    */
  private def isCollectionOfStructs(value: Any): Boolean = value match {
    case value: java.util.Map[_, _] =>
      value.asScala.collectFirst {
        case (_, _: Struct) => ()
      }.nonEmpty

    case value: java.util.Collection[_] =>
      value.asScala.collectFirst {
        case _: Struct => ()
      }.nonEmpty
  }

  private val ConverterTopicName = "irrelevant-topic-name"

  private case class FieldNode(path: Seq[String], value: Any, schema: Schema) {
    def pathAsString: String = path.mkString(pathDelimiter)
  }
}

object Flattener {
  def flatten(
    value:          Any,
    originalSchema: Schema,
  )(
    implicit
    config: FlattenerConfig,
  ): Any = new Flattener(config.discardCollections).flatten(value, originalSchema)
}
