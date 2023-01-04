/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.{Field, Schema, Struct}

import java.util
import scala.jdk.CollectionConverters._

object Flattener extends LazyLogging {

  private val jacksonMapper = new ObjectMapper()

  private case class FieldNode(path: Seq[String], value: AnyRef)

  /**
    * Flattens a Kafka Connect record value
    *
    * @param value A kafka connect record value
    * @param flattenedSchema a flattened schema (i.e. the representation of target record shape).
    * @param config
    * @return
    */
  def flatten(
    value:           AnyRef,
    flattenedSchema: Schema,
  )(
    implicit
    config: FlattenerConfig,
  ): AnyRef = {
    def go(path: Seq[String], value: AnyRef): Vector[FieldNode] =
      value match {
        case value: Struct =>
          val structFields = Option(value.schema()).map(_.fields().asScala)
            .getOrElse(List.empty[Field])

          structFields.foldLeft(Vector.empty[FieldNode]) { (acc, field) =>
            val newPath = path :+ field.name()
            acc ++ go(newPath, value.get(field.name()))
          }

        case _ if isCollectionOrMap(value) =>
          if (config.discardCollections)
            Vector.empty
          else
            Vector(FieldNode(path, jacksonMapper.writeValueAsString(value)))

        case _ =>
          Vector(FieldNode(path, value))
      }

    //do nothing if top-level schema is not a record
    if (flattenedSchema.`type`() != Schema.Type.STRUCT)
      value
    else {
      val newStruct = new Struct(flattenedSchema)
      config.jsonBlobChunks.fold(
        go(Vector.empty, value).foldLeft(newStruct) { (struct, field) =>
          val fieldName = field.path.mkString("_")
          struct.put(fieldName, field.value)
        },
      ) { implicit blobConfig =>
        ChunkedJsonBlob.asConnectData(value)
      }
    }
  }


  private def isCollectionOrMap(value: AnyRef): Boolean =
    value.isInstanceOf[util.Collection[_]] || value.isInstanceOf[util.Map[_, _]]
}
