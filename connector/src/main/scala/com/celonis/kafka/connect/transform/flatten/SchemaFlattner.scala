package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.Field
import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.FlatSchema
import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

final class SchemaFlattner(discardCollections: Boolean) {
  def flatten(schema: Schema): FlatSchema = FlatSchema(flatten(Path.empty, schema))

  private def flatten(path: Path, schema: Schema): List[Field] = schema.`type`() match {
    case Schema.Type.STRUCT => schema.fields().asScala.toList.flatMap {
        field => flatten(path.append(field.name()), field.schema())
      }

    // At the top level, array and maps are returned unchanged (I don't think it is correct behaviour, here for BC)
    case Schema.Type.ARRAY | Schema.Type.MAP if path.path.isEmpty =>
      List(Field(Path.empty, schema))

    case Schema.Type.ARRAY | Schema.Type.MAP if discardCollections => Nil

    // TODO: top level array and maps should be returned as they are
    case Schema.Type.ARRAY | Schema.Type.MAP => List(Field(path, Schema.OPTIONAL_STRING_SCHEMA))

    case primitive => List(Field(path, new SchemaBuilder(primitive).optional().build()))
  }
}

object SchemaFlattner {
  case class Path(path: Vector[String]) {
    def name: String = path.mkString("_")
    def append(segment:     String): Path = Path(path :+ segment)
    def extractValue(value: Any): Any =
      path match {
        case head +: tail => value match {
            case struct: Struct              => Path(tail).extractValue(struct.get(head))
            case map:    java.util.Map[_, _] => Path(tail).extractValue(map.get(head))
            case _ => ???
          }
        case _ => value
      }
  }
  object Path {
    val empty: Path = Path(Vector.empty)
  }

  case class Field(path: Path, schema: Schema)

  case class FlatSchema(fields: List[Field]) {
    def connectSchema: Schema =
      fields match {
        case field :: Nil if field.path.path.isEmpty => field.schema
        case _ =>
          fields.foldLeft(SchemaBuilder.struct())((builder, field) =>
            builder.field(field.path.name, field.schema),
          ).build()
      }

  }
}

final class Flattenr {
  def flatten(value: Any, flatSchema: FlatSchema): Any = {
    val schema = flatSchema.connectSchema
    val struct = new Struct(schema)
    flatSchema.fields.foreach(field => struct.put(field.path.name, field.path.extractValue(value)))
    struct
  }
}

object Flattenr {
  case class FieldValue(path: Path, value: Any, schema: Schema)

}
