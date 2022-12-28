/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.SchemaLeafNode
import com.celonis.kafka.connect.transform.flatten.LogicalTypes.logicalTypeSchemas
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object StructSchemaFlattener {

  private val primitiveTypes: Map[Schema.Type, Schema] = Map(
    INT8    -> Schema.OPTIONAL_INT8_SCHEMA,
    INT16   -> Schema.OPTIONAL_INT16_SCHEMA,
    INT32   -> Schema.OPTIONAL_INT32_SCHEMA,
    INT64   -> Schema.OPTIONAL_INT64_SCHEMA,
    FLOAT32 -> Schema.OPTIONAL_FLOAT32_SCHEMA,
    FLOAT64 -> Schema.OPTIONAL_FLOAT64_SCHEMA,
    BOOLEAN -> Schema.OPTIONAL_BOOLEAN_SCHEMA,
    STRING  -> Schema.OPTIONAL_STRING_SCHEMA,
    BYTES   -> Schema.OPTIONAL_BYTES_SCHEMA,
  )

  private def complexExtractors(
                                 implicit
                                 config: FlattenerConfig,
  ): Map[Schema.Type, (Schema, Seq[String]) => Seq[SchemaLeafNode]] = Map(
    (ARRAY,
     (_, path) =>
       if (config.discardCollections)
         Seq.empty
       else
         Seq(SchemaLeafNode(path, Schema.Type.STRING, Schema.OPTIONAL_STRING_SCHEMA)),
    ),
    (MAP, (_, _) => Seq.empty),
    (STRUCT, (schema, path) => StructSchemaFlattener.flatten(path, schema)),
  )

  def flatten(path: Seq[String], schemaToFlatten: Schema)(implicit config: FlattenerConfig): Seq[SchemaLeafNode] = {
    for {
      schema      <- Option(schemaToFlatten)
      fields      <- Try(schema.fields()).toOption
      fieldsScala <- Try(fields.asScala).toOption
    } yield fieldsScala
  }.getOrElse(Seq.empty)
    .flatMap {
      field: Field =>
        val sType      = field.schema().`type`()
        val schemaName = Try(field.schema().name()).toOption
        if (schemaName.exists(logicalTypeSchemas.keySet.contains)) {
          Seq(SchemaLeafNode(path :+ field.name(), sType, field.schema()))
        } else if (primitiveTypes.contains(sType)) {
          val primOptSchema = primitiveTypes(sType)
          Seq(SchemaLeafNode(path :+ field.name(), sType, primOptSchema))
        } else if (complexExtractors.contains(sType)) {
          val r: Option[Seq[SchemaLeafNode]] =
            complexExtractors.get(sType).map(_.apply(field.schema(), path :+ field.name()))
          r.getOrElse(Seq.empty)
        } else {
          throw new IllegalArgumentException(s"Unknown type: $sType")
        }
    }.toSeq
}
