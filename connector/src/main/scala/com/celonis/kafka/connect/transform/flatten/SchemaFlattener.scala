/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.flatten.SchemaFlattener.Field
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener.FlatSchema
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import scala.jdk.CollectionConverters._

private final class SchemaFlattener(discardCollections: Boolean) {
  def flatten(schema: Schema): FlatSchema = FlatSchema(flatten(Path.empty, schema))

  private def flatten(path: Path, schema: Schema): List[Field] = schema.`type`() match {
    case Schema.Type.STRUCT => schema.fields().asScala.toList.flatMap {
        field => flatten(path.append(field.name()), field.schema())
      }

    // At the top level, array and maps are returned unchanged (I don't think it is correct behaviour, here for BC)
    case Schema.Type.ARRAY | Schema.Type.MAP if path.segments.isEmpty =>
      List(Field(Path.empty, schema))

    case Schema.Type.ARRAY | Schema.Type.MAP if discardCollections => Nil

    // TODO: top level array and maps should be returned as they are
    case Schema.Type.ARRAY | Schema.Type.MAP => List(Field(path, Schema.OPTIONAL_STRING_SCHEMA))

    case primitive => List(Field(path, new SchemaBuilder(primitive).optional().build()))
  }
}

private object SchemaFlattener {
  final case class FlatSchema(fields: List[Field]) {
    def connectSchema: Schema =
      fields match {
        case field :: Nil if field.path.segments.isEmpty => field.schema
        case _ =>
          fields.foldLeft(SchemaBuilder.struct())((builder, field) =>
            builder.field(field.path.name, field.schema),
          ).build()
      }
  }

  final case class Field(path: Path, schema: Schema)

  final case class Path(segments: Vector[String]) {
    def name: String = segments.mkString("_")
    def append(segment: String): Path = Path(segments :+ segment)
  }

  object Path {
    val empty: Path = Path(Vector.empty)
  }
}
