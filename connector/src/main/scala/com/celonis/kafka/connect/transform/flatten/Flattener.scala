/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

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
          if (schemaIsInferred) {
            value.asScala.toVector.foldLeft(Vector.empty[FieldNode]) {
              case (acc, (key, value)) =>
                val newPath = path :+ key.toString()
                acc ++ go(newPath, value)
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
    fields.foldLeft(Map.empty[String, Any]) { (m, field) =>
      val fieldName = field.path.mkString("_")
      m.updated(fieldName, field.value)
    }.asJava

  private def structFrom(fields: Vector[FieldNode], flatSchema: Schema): Struct =
    fields.foldLeft(new Struct(flatSchema)) { (struct, field) =>
      val fieldName = field.path.mkString("_")
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
    else
      Vector(FieldNode(path, jacksonMapper.writeValueAsString(value)))

  private val jacksonMapper = new ObjectMapper()

  private case class FieldNode(path: Seq[String], value: Any)
}
