/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

object Flattener extends LazyLogging {

  /**
    * Flattens a Kafka Connect record value
    *
    * @param value A kafka connect record value
    * @param flattenedSchema a flattened schema (i.e. the representation of target record shape).
    * @param schemaIsInferred when true, flattening function will treat incoming Map values as records,
    *                         rather than simply json-encoding or dropping them.
    * @param config The flattener configuration.
    * @return
    */
  def flatten(
    value:            Any,
    flattenedSchema:  Schema,
    schemaIsInferred: Boolean = false,
  )(
    implicit
    config: FlattenerConfig,
  ): Any = {
    def go(path: Seq[String], value: Any): Vector[FieldNode] =
      value match {
        case value: Struct =>
          val structFields = Option(value.schema()).map(_.fields().asScala)
            .getOrElse(List.empty[Field])

          structFields.foldLeft(Vector.empty[FieldNode]) { (acc, field) =>
            val newPath = path :+ field.name()
            acc ++ go(newPath, value.get(field.name()))
          }

        case value: Map[_, _] =>
          go(path, value.asJava)

        case value: java.util.Map[_, _] =>
          if (schemaIsInferred && !fieldExists(flattenedSchema, path)) {
            value.asScala.toVector.flatMap {
              case (key, value) => go(path :+ key.toString, value)
            }
          } else discardOrJsonEncodeCollection(value, path)

        case _: java.util.Collection[_] =>
          discardOrJsonEncodeCollection(value, path)

        case _ =>
          Vector(FieldNode(path, value))
      }

    //do nothing if top-level schema is not a record
    if (flattenedSchema.`type`() != Schema.Type.STRUCT)
      value
    else {
      config.jsonBlobChunks.fold {
        val fields = go(Vector.empty, value)

        if (schemaIsInferred)
          hashMapFrom(fields)
        else
          structFrom(fields, flattenedSchema)

      } { implicit blobConfig =>
        ChunkedJsonBlob.asConnectData(value)
      }
    }
  }

  private def hashMapFrom(fields: Vector[FieldNode]): java.util.Map[String, Any] =
    fields.map(fieldNode => fieldNode.pathAsString -> fieldNode.value).toMap.asJava

  private def structFrom(fields: Vector[FieldNode], flatSchema: Schema): Struct =
    fields.foldLeft(new Struct(flatSchema)) { (struct, field) =>
      val fieldName = field.pathAsString
      struct.put(fieldName, field.value)
    }

  private def discardOrJsonEncodeCollection(
    value: Any,
    path:  Seq[String],
  )(
    implicit
    config: FlattenerConfig,
  ): Vector[FieldNode] =
    if (config.discardCollections)
      Vector.empty
    else {
      val schema = inferCollectionSchema(value).orNull
      val json = new String(ConnectJsonConverter.converter.fromConnectData(ConverterTopicName, schema, value),
                            StandardCharsets.UTF_8,
      )
      Vector(FieldNode(path, json))
    }

  private def fieldExists(schema: Schema, path: Seq[String]): Boolean = schema.`type`() match {
    case Schema.Type.STRUCT => schema.field(path.mkString(pathDelimiter)) != null
    case _                  => false
  }

  private def inferCollectionSchema(value: Any): Option[Schema] = value match {
    case value: java.util.Map[_, _] =>
      value.asScala.collectFirst {
        case (key, struct: Struct) =>
          val keySchema = inferPrimitive(key).getOrElse(Schema.STRING_SCHEMA)
          SchemaBuilder.map(keySchema, struct.schema()).build()
      }

    case value: java.util.Collection[_] =>
      value.asScala.collectFirst {
        case struct: Struct => SchemaBuilder.array(struct.schema()).build()
      }
  }

  private def inferPrimitive(v: Any): Option[Schema] = v match {
    case _: String => Some(Schema.STRING_SCHEMA)
    case _: Int    => Some(Schema.INT32_SCHEMA)
    case _: Long   => Some(Schema.INT32_SCHEMA)
    case _: Float | _: Double | _: BigDecimal => Some(Schema.FLOAT64_SCHEMA)
    case _: Boolean     => Some(Schema.BOOLEAN_SCHEMA)
    case _: Array[Byte] => Some(Schema.BYTES_SCHEMA)
    case _ => None
  }

  private val ConverterTopicName = "irrelevant-topic-name"

  private case class FieldNode(path: Seq[String], value: Any) {
    def pathAsString: String = path.mkString(pathDelimiter)
  }

  private val pathDelimiter = "_"
}
