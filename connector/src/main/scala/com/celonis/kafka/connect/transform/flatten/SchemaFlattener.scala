/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.clean.PathCleaner.cleanPath
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import scala.jdk.CollectionConverters._

object SchemaFlattener {

  private implicit class SchemaBuilderExt(sb: SchemaBuilder)(implicit config: FlattenConfig) {
    def mergeFields(schema: Schema): SchemaBuilder =
      if (schema.`type`() != STRUCT)
        sb
      else
        schema.fields().asScala.foldLeft(sb) { (sb, field) =>
          sb.field(field.name(), field.schema())
        }
  }

  private implicit class FieldExt(field: Field) {
    def discardCollectionAsPerConfig(implicit config: FlattenConfig): Boolean =
      config.discardCollections && Set(MAP, ARRAY).contains(field.schema().`type`())
  }

  def flatten(schema: Schema)(implicit config: FlattenConfig): Schema = {
    def go(path: Vector[String])(schema: Schema): Schema =
      schema.`type`() match {
        case INT8 | INT16 | INT32 | INT64 | FLOAT32 | FLOAT64 | BOOLEAN | STRING | BYTES =>
          asOptionalPrimitive(schema)

        case ARRAY | MAP =>
          if (path.isEmpty)
            schema
          else
            Schema.OPTIONAL_STRING_SCHEMA

        case STRUCT =>
          schema.fields().asScala.filterNot(_.discardCollectionAsPerConfig).foldLeft(SchemaBuilder.struct()) {
            (sb, field) =>
              val fieldPath  = path :+ field.name()
              val fieldName  = fieldNameFromPath(fieldPath)
              val fieldValue = go(fieldPath)(field.schema())

              if (fieldValue.`type`() == STRUCT)
                sb.mergeFields(fieldValue)
              else
                sb.field(fieldName, fieldValue)

          }.build()

        case other =>
          throw new IllegalArgumentException(s"Unexpected schema type $other")
      }

    go(Vector.empty)(schema)
  }

  private def asOptionalPrimitive(schema: Schema): Schema =
    if (schema.isOptional || schema.`type`().isPrimitive)
      primitiveOptionals.getOrElse(schema.`type`(), throw new IllegalArgumentException("Expected primitive"))
    else
      schema

  private val primitiveOptionals = Map(
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

  private[flatten] def fieldNameFromPath(path: Vector[String])(implicit config: FlattenConfig) =
    cleanPath(path).mkString("_")

}
