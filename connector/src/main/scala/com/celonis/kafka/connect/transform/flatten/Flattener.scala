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
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object Flattener extends LazyLogging {

  /**
    * Flattens a Kafka Connect record value
    *
    * @param value A kafka connect record value
    * @param flattenedSchema a flattened schema (i.e. the representation of target record shape).
    * @param config The flattener configuration.
    * @return
    */
  def flatten(
    value:           Any,
    originalSchema:  Schema,
    flattenedSchema: Schema,
  )(
    implicit
    config: FlattenerConfig,
  ): Any = {
    def toFieldNodes(path: Seq[String], value: Any): Vector[FieldNode] =
      value match {
        case value: Struct =>
          val structFields = value.schema.fields.asScala.toVector
          structFields.flatMap(field => toFieldNodes(path :+ field.name(), value.get(field.name())))

        case value: java.util.Map[_, _] =>
          if (fieldExists(originalSchema, path.toList)) {
            value.asScala.toVector.flatMap {
              case (key, value) => toFieldNodes(path :+ key.toString, value)
            }
          } else discardOrJsonEncodeCollection(value, path)

        case _: java.util.List[_] =>
          discardOrJsonEncodeCollection(value, path)

        case _ =>
          Vector(FieldNode(path, value))
      }

    //do nothing if top-level schema is not a record
    if (originalSchema.`type` != Schema.Type.STRUCT)
      value
    else {
      config.jsonBlobChunks.fold {
        val fields = toFieldNodes(Vector.empty, value)

        structFrom(fields, flattenedSchema)

      } { implicit blobConfig =>
        ChunkedJsonBlob.asConnectData(value)
      }
    }
  }

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

  @tailrec
  private def fieldExists(originalSchema: Schema, path: List[String]): Boolean =
    path match {
      case _ if originalSchema.`type` != Schema.Type.STRUCT   => false
      case head :: tail if originalSchema.field(head) != null => fieldExists(originalSchema.field(head).schema(), tail)
      case _ :: _                                             => false
      case Nil                                                => true
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
}
