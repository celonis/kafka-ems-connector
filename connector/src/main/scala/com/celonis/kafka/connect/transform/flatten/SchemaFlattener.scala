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

    case primitive =>
      val newSchema =
        new SchemaBuilder(primitive).optional()
      if (schema.parameters() != null) newSchema.parameters(schema.parameters())
      if (schema.name() != null) newSchema.name(schema.name())
      if (schema.version() != null) newSchema.version(schema.version())
      List(Field(path, newSchema.build()))
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
    // We make the flat version of a name avro-complaint, replacing unallowed characters with an underscore
    def name: String = segments.mkString("_").replaceAll("[^a-zA-Z0-9]", "_")
    def append(segment: String): Path = Path(segments :+ segment)
  }

  object Path {
    val empty: Path = Path(Vector.empty)
  }
}
