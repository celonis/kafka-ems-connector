/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.storage.ConverterType

import java.nio.charset.StandardCharsets
import java.util
import scala.jdk.CollectionConverters._

object Flattener extends LazyLogging {
  case class MisconfiguredJsonBlobMaxChunks(configuredChunksSize: Int, blobByteSize: Int, emsVarcharLength: Int)
      extends Throwable {
    override def getMessage: String =
      s"Configured value ${configuredChunksSize} for ${FlattenerConfig.JsonBlobMaxChunks} is insufficient! Current JSON blob length: $blobByteSize, Ems VARCHAR Length: ${emsVarcharLength}"
  }

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
      ) {
        case FlattenerConfig.JsonBlobChunks(maxChunks, emsVarcharLength) =>
          val struct        = value.asInstanceOf[Struct]
          val jsonBlobBytes = jsonConverter.fromConnectData("some-topic", struct.schema(), struct)
          val numChunks     = jsonBlobBytes.length / emsVarcharLength
          if (numChunks > maxChunks)
            throw MisconfiguredJsonBlobMaxChunks(maxChunks, jsonBlobBytes.length, emsVarcharLength)
          else
            jsonBlobBytes.grouped(emsVarcharLength).zipWithIndex.foldLeft(newStruct) {
              case (newStruct, (jsonBlobChunk, idx)) =>
                newStruct.put(s"payload_chunk${idx + 1}", new String(jsonBlobChunk, StandardCharsets.UTF_8))
            }
      }
    }
  }

  private[flatten] lazy val jsonConverter = {
    val converter = new JsonConverter()
    converter.configure(
      Map(
        "converter.type" -> ConverterType.VALUE.getName,
        "schemas.enable" -> "false",
      ).asJava,
    )
    converter

  }

  private def isCollectionOrMap(value: AnyRef): Boolean =
    value.isInstanceOf[util.Collection[_]] || value.isInstanceOf[util.Map[_, _]]
}
