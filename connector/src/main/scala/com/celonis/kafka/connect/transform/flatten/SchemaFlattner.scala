package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.Field
import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.FlatSchema
import com.celonis.kafka.connect.transform.flatten.SchemaFlattner.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.nio.charset.StandardCharsets
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

final class Flattner(discardCollections: Boolean) {
  def flatten(value: Any, flatSchema: FlatSchema): Any = {
    val schema = flatSchema.connectSchema

    schema.`type`() match {
      case Schema.Type.STRUCT =>
        flatSchema.fields.foldLeft(new Struct(schema))(processField(value))
      case _ => value
    }
  }

  private def processField(value: Any)(struct: Struct, field: Field): Struct = {
    val extractedValue = field.path.extractValue(value)
    val fieldValue = extractedValue match {
      case _: java.util.Map[_, _] | _: java.util.Collection[_] =>
        jsonEncodeCollection(extractedValue, field.path.path, field.schema)
      case _ => extractedValue
    }
    struct.put(field.path.name, fieldValue)
    struct
  }

  private def jsonEncodeCollection(
    value:  Any,
    path:   Seq[String],
    schema: Schema,
  ): String = {
    val convertSchema = schemaOfCollection(value).orNull
    new String(
      ConnectJsonConverter.converter.fromConnectData(ConverterTopicName, convertSchema, value),
      StandardCharsets.UTF_8,
    )
  }

  /**
    * Weird hack due to the fact that the connect json converter does not handle `Map`s as structs.
    * This should go once we are converting maps into structs upstream
    */
  private def schemaOfCollection(value: Any): Option[Schema] = value match {
    case value: java.util.Map[_, _] =>
      value.asScala.collectFirst {
        case (_, struct: Struct) =>
          SchemaBuilder.map(Schema.STRING_SCHEMA, struct.schema()).build()
      }

    case value: java.util.Collection[_] =>
      value.asScala.collectFirst {
        case struct: Struct =>
          SchemaBuilder.array(struct.schema()).build()
      }

    case _ => None
  }

  private val ConverterTopicName = "irrelevant-topic-name"
}
