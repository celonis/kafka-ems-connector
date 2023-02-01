package com.celonis.kafka.connect.transform.flatten

import cats.data.NonEmptyList
import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.{Field, FlatSchema, Path}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import scala.jdk.CollectionConverters._

final class SchemaFlattner {
  def flatten(schema: Schema): FlatSchema = FlatSchema(flatten(Path.empty, schema))

  private def flatten(path: Path, schema: Schema): List[Field] = schema.`type`() match {
    case Schema.Type.STRUCT => schema.fields().asScala.toList.flatMap{
      field => flatten(path.append(field.name()), field.schema())
    }
    case Schema.Type.ARRAY =>
    case Schema.Type.MAP =>
    case _ => List(Field(path, schema))
  }
}

object SchemaFlattner {
  case class Path(path: Vector[String]) {
    def name: String = path.mkString("_")
    def append(segment: String): Path = Path(path :+ segment)
    def extractValue(value: Any): Any = ???
  }
  object Path {
    val empty: Path = Path(Vector.empty)
  }


  case class Field(path: Path, schema: Schema)

  case class FlatSchema(fields: List[Field]) {
    def connectSchema: Schema = ???
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
