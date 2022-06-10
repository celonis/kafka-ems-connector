/*
 * Copyright 2022 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.ems.conversion

import cats.syntax.either._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

/** *
  * For PK settings we need an orderable field which allows the EMS ingestion to de-duplicate records in the same file.
  * We will be using the record partition + offset to ensure the order
  */
sealed trait OrderFieldInserter {
  def add(value: Any, partition: Int, offset: Long): Either[Throwable, Any]
}

object NoOpOrderFieldInserter extends OrderFieldInserter {
  override def add(value: Any, partition: Int, offset: Long): Either[Throwable, Any] = value.asRight
}
object OrderFieldInserter extends OrderFieldInserter {
  val FieldName: String = "__celonis_order"

  override def add(value: Any, partition: Int, offset: Long): Either[Throwable, Any] = {
    val sortableValue = CreateOrderFieldValue(partition, offset)
    value match {
      case struct: Struct => StructInsertField.apply(struct, FieldName, sortableValue).asRight
      case map:    Map[_, _] =>
        val newMap = map.asInstanceOf[Map[String, _]] + (FieldName -> sortableValue)
        newMap.asRight[Throwable]
      case map: java.util.Map[_, _] =>
        map.asInstanceOf[java.util.Map[String, Any]].put(FieldName, sortableValue)
        map.asRight
      case other =>
        InvalidInputException(
          s"Invalid input received. To write the data to Parquet files the input needs to be an Object but found:${other.getClass.getCanonicalName}.",
        ).asLeft
    }
  }
}

object StructInsertField {

  import org.apache.kafka.common.cache.Cache

  private val cache: Cache[Schema, Schema] = new LRUCache[Schema, Schema](16)

  def copySchemaBasics(source: Schema, builder: SchemaBuilder): SchemaBuilder = {
    builder.name(source.name)
    builder.version(source.version)
    builder.doc(source.doc)
    Option(source.parameters).foreach(builder.parameters)
    builder
  }

  def makeUpdatedSchema(schema: Schema, fieldName: String): Schema = {
    val builder: SchemaBuilder = copySchemaBasics(schema, SchemaBuilder.struct())

    for (field <- schema.fields.asScala) {
      builder.field(field.name, field.schema)
    }

    builder.field(fieldName, Schema.INT64_SCHEMA)
    builder.build()
  }

  def apply(struct: Struct, fieldName: String, value: Long): Struct = {
    val updatedSchema = Option(cache.get(struct.schema())) match {
      case Some(value) => value
      case None =>
        val newSchema = makeUpdatedSchema(struct.schema(), fieldName)
        cache.put(struct.schema(), newSchema)
        newSchema
    }
    val newStruct = new Struct(updatedSchema)
    struct.schema().fields().asScala.foreach { field =>
      newStruct.put(field.name(), struct.get(field))
    }
    newStruct.put(fieldName, value)
    newStruct
  }
}

object CreateOrderFieldValue {

  /**
    * Creates the value for the automatic ordered field. A record order guarantee in Kafka is provided at the partition level only.
    * Therefore, we can use the Kafka message offset value as the orderable value. EMS will be able to deduplicate if there are more than one entries
    * in the same file for a given PK
    * @param partition - The Kafka topic partition value
    * @param offset - The Kafka message offset
    * @return
    */
  def apply(partition: Int, offset: Long): Long = offset
}
