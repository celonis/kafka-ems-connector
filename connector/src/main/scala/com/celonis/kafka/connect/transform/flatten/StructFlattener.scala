/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.LeafNode
import com.celonis.kafka.connect.transform.Node
import com.celonis.kafka.connect.transform.flatten.LogicalTypes.logicalTypeSchemas
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

object StructFlattener {

  private val primitiveExtractors: Map[Schema.Type, (Struct, String) => AnyRef] = Map(
    (INT8, (struct, fieldName) => struct.getInt8(fieldName)),
    (INT16, (struct, fieldName) => struct.getInt16(fieldName)),
    (INT32, (struct, fieldName) => struct.getInt32(fieldName)),
    (INT64, (struct, fieldName) => struct.getInt64(fieldName)),
    (FLOAT32, (struct, fieldName) => struct.getFloat32(fieldName)),
    (FLOAT64, (struct, fieldName) => struct.getFloat64(fieldName)),
    (BOOLEAN, (struct, fieldName) => struct.getBoolean(fieldName)),
    (STRING, (struct, fieldName) => struct.getString(fieldName)),
    (BYTES, (struct, fieldName) => struct.getBytes(fieldName)),
  )

  private def complexExtractors(
                                 implicit
                                 config: FlattenerConfig,
  ): Map[Schema.Type, (Struct, String, Seq[String]) => Seq[Node]] = Map(
    (ARRAY,
     (struct, fieldName, path) =>
       ArrayFlattener.flatten(path :+ fieldName,
                              struct.getArray(fieldName),
                              Try(struct.schema().field(fieldName).schema()).toOption,
       ),
    ),
    (MAP, (struct, fieldName, path) => MapFlattener.flatten(path :+ fieldName, mapToScala(struct.getMap(fieldName)))),
    (STRUCT, (struct, fieldName, path) => StructFlattener.flatten(path :+ fieldName, struct.getStruct(fieldName))),
  )

  def flatten(path: Seq[String], objectToFlatten: Struct)(implicit config: FlattenerConfig): Seq[Node] = {
    for {
      struct       <- Option(objectToFlatten)
      schema       <- Try(struct.schema()).toOption
      schemaFields <- Try(schema.fields().asScala).toOption
    } yield schemaFields
  }.getOrElse(Seq.empty)
    .flatMap(flattenField(path, objectToFlatten, _))
    .toSeq

  private def extractLogicalTypes(path: Seq[String], objectToFlatten: Struct, field: Field): Option[Seq[Node]] =
    Try(field.schema().name()).toOption
      .filter(logicalTypeSchemas.keySet.contains)
      .map(_ => Seq(LeafNode(path :+ field.name(), objectToFlatten.get(field))))

  private def flattenField(
    path:            Seq[String],
    objectToFlatten: Struct,
    field:           Field,
  )(
                            implicit
                            config: FlattenerConfig,
  ): Seq[Node] =
    extractLogicalTypes(path, objectToFlatten, field)
      .orElse {
        primitiveExtractors
          .get(field.schema().`type`())
          .map(f => Seq(LeafNode(path :+ field.name, f(objectToFlatten, field.name()))))
      }
      .orElse {
        complexExtractors
          .get(field.schema().`type`())
          .map(_(objectToFlatten, field.name(), path))
      }
      .getOrElse(Seq.empty[Node])

  private def mapToScala(map: util.Map[_, _]): Map[Any, Any] = map.asScala.toMap

}
