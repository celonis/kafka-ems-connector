/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion
import org.apache.avro.Schema

import scala.jdk.CollectionConverters._

object SchemaExtensions {
  implicit class SchemaHelper(val schema: Schema) extends AnyVal {
    def nonNullableSchema: Option[Schema] =
      schema.getTypes.asScala.find(_.getType != Schema.Type.NULL)

    def isRecord: Boolean = schema.getType == Schema.Type.RECORD
  }
}
