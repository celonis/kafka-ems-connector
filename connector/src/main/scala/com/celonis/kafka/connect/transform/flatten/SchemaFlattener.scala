/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.SchemaLeafNode
import com.celonis.kafka.connect.transform.clean.PathCleaner.cleanPath
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

object SchemaFlattener {

  def flatten(maybeSchema: Option[Schema])(implicit config: FlattenConfig): Either[String, Option[Schema]] =
    (maybeSchema, maybeSchema.map(_.`type`())) match {
      case (_, Some(INT16))   => maybeSchema.asRight
      case (_, Some(INT32))   => maybeSchema.asRight
      case (_, Some(INT64))   => maybeSchema.asRight
      case (_, Some(FLOAT32)) => maybeSchema.asRight
      case (_, Some(FLOAT64)) => maybeSchema.asRight
      case (_, Some(BOOLEAN)) => maybeSchema.asRight
      case (_, Some(STRING))  => maybeSchema.asRight
      case (_, Some(BYTES))   => maybeSchema.asRight
      //TOP level array schema is handled as an optional string?
      case (_, Some(ARRAY)) =>
        if (config.discardCollections)
          None.asRight
        else
          Schema.OPTIONAL_STRING_SCHEMA.some.asRight
      case (_, Some(MAP)) =>
          maybeSchema.asRight

      case (Some(schema), Some(STRUCT)) => flattenStructSchema(schema).some.asRight
      case (_, None)                    => Option.empty.asRight
      case other                        => s"Unexpected type $other".asLeft
    }

  def flattenStructSchema(schema: Schema)(implicit config: FlattenConfig): Schema =
    StructSchemaFlattener.flatten(Seq.empty, schema).foldLeft(SchemaBuilder.struct()) {
      case (s: SchemaBuilder, node: SchemaLeafNode) =>
        s.field(cleanPath(node.path).mkString("_"), node.value)
    }.build()

}
