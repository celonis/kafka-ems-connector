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
import cats.implicits._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Struct

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

sealed trait DataConverter[T] {
  def convert(value: T): Either[Throwable, GenericRecord]
}

object DataConverter {
  def apply(value: Any): Either[Throwable, GenericRecord] =
    value match {
      case struct: Struct              => StructDataConverter.convert(struct)
      case map:    Map[_, _]           => MapDataConverter.convert(map)
      case map:    java.util.Map[_, _] => MapDataConverter.convert(map.asScala.toMap)
      case other =>
        InvalidInputException(
          s"Invalid input received. To write the data to Parquet files the input needs to be an Object but found:${other.getClass.getCanonicalName}.",
        ).asLeft
    }
}

object StructDataConverter extends DataConverter[Struct] {
  private val avroDataConverter = new AvroData(100)
  override def convert(struct: Struct): Either[Throwable, GenericRecord] =
    Try(avroDataConverter.fromConnectData(struct.schema(), struct)).map(_.asInstanceOf[GenericRecord]).toEither
}

object MapDataConverter extends DataConverter[Map[_, _]] {
  override def convert(map: Map[_, _]): Either[Throwable, GenericRecord] = convert(map, "root")

  private def convertValue(
    value:         Any,
    key:           String,
    vector:        Vector[FieldAndValue],
    fieldsBuilder: SchemaBuilder.FieldAssembler[Schema],
  ): Either[Throwable, Vector[FieldAndValue]] =
    value match {
      case s: String =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().stringType()).noDefault()
        (vector :+ FieldAndValue(key, s)).asRight
      case l: Long =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().longType()).noDefault()
        (vector :+ FieldAndValue(key, java.lang.Long.valueOf(l))).asRight
      case i: Int =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().intType()).noDefault()
        val newVector = vector :+ FieldAndValue(key, java.lang.Integer.valueOf(i))
        newVector.asRight
      case b: Boolean =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().booleanType()).noDefault()
        val newVector = vector :+ FieldAndValue(key, java.lang.Boolean.valueOf(b))
        newVector.asRight
      case f: Float =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().floatType()).noDefault()
        val newVector = vector :+ FieldAndValue(key, java.lang.Float.valueOf(f))
        newVector.asRight
      case d: Double =>
        fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().doubleType()).noDefault()
        val newVector = vector :+ FieldAndValue(key, java.lang.Double.valueOf(d))
        newVector.asRight
      case list: java.util.List[_] =>
        createSchema(list.asScala).map {
          case (schema, value) =>
            fieldsBuilder.name(key).`type`(schema).noDefault()
            vector :+ FieldAndValue(key, value)
        }
      case list: List[_] =>
        createSchema(list).map {
          case (schema, value) =>
            fieldsBuilder.name(key).`type`(schema).noDefault()
            vector :+ FieldAndValue(key, value)
        }
      case innerMap: java.util.Map[_, _] =>
        convert(innerMap.asScala.toMap, key).map { innerStruct =>
          fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().`type`(innerStruct.getSchema)).noDefault()
          vector :+ FieldAndValue(key, innerStruct)
        }
      case innerMap: Map[_, _] =>
        convert(innerMap, key).map { innerStruct =>
          fieldsBuilder.name(key).`type`(SchemaBuilder.builder().nullable().`type`(innerStruct.getSchema)).noDefault()
          vector :+ FieldAndValue(key, innerStruct)
        }

      case other =>
        new RuntimeException(
          s"Converting schemaless JSON does not handle type:${other.getClass.getCanonicalName}",
        ).asLeft
    }

  private def createSchema(value: Any): Either[Throwable, (Schema, AnyRef)] =
    value match {
      case b:    Boolean => (SchemaBuilder.builder.booleanType()  -> java.lang.Boolean.valueOf(b)).asRight
      case i:    Int     => (SchemaBuilder.builder().intType()    -> java.lang.Integer.valueOf(i)).asRight
      case l:    Long    => (SchemaBuilder.builder().longType()   -> java.lang.Long.valueOf(l)).asRight
      case f:    Float   => (SchemaBuilder.builder().floatType()  -> java.lang.Float.valueOf(f)).asRight
      case d:    Double  => (SchemaBuilder.builder().floatType()  -> java.lang.Double.valueOf(d)).asRight
      case s:    Char    => (SchemaBuilder.builder().stringType() -> s.toString).asRight
      case s:    String => (SchemaBuilder.builder().stringType() -> s).asRight
      case list: mutable.Buffer[_] =>
        list.toList.traverse(createSchema).flatMap { convertedFields =>
          val firstItemSchemaEither =
            if (list.isEmpty) SchemaBuilder.builder().nullable().stringType().asRight[Throwable]
            else createSchema(list.head).map(_._1)
          firstItemSchemaEither.map { firstItemSchema =>
            val schema = SchemaBuilder.builder().nullable().array().items(firstItemSchema)
            val array =
              new GenericData.Array[AnyRef](list.size, schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get)
            convertedFields.map(_._2).foreach(array.add)
            schema -> array
          }
        }

      case list: List[_] =>
        list.traverse(createSchema).flatMap { convertedFields =>
          val firstItemSchemaEither =
            if (list.isEmpty) SchemaBuilder.builder().nullable().stringType().asRight[Throwable]
            else createSchema(list.head).map(_._1)
          firstItemSchemaEither.map { firstItemSchema =>
            val schema = SchemaBuilder.builder().nullable().array().items(firstItemSchema)
            val array =
              new GenericData.Array[AnyRef](list.size, schema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get)
            convertedFields.map(_._2).foreach(array.add)
            schema -> array
          }
        }

      case other =>
        new RuntimeException(
          s"Converting schemaless JSON does not handle type:${other.getClass.getCanonicalName}",
        ).asLeft
    }

  private def convert(map: Map[_, _], key: String): Either[Throwable, GenericRecord] =
    if (map.isEmpty)
      InvalidInputException(
        s"Invalid input received. The connector has received an empty input which cannot be written to Parquet. This can happen for empty JSON objects. ",
      ).asLeft
    else {
      val builder:       SchemaBuilder.RecordBuilder[Schema]  = SchemaBuilder.builder().record(key)
      val fieldsBuilder: SchemaBuilder.FieldAssembler[Schema] = builder.fields()
      map.toList.sortBy(_._1.toString)
        .foldLeft(Vector.empty[FieldAndValue].asRight[Throwable]) {
          case (acc, (k, v)) =>
            acc.flatMap(vector => convertValue(v, asAvroCompliantName(k.toString), vector, fieldsBuilder))
        }.map { fieldsAndValues =>
          val schema = fieldsBuilder.endRecord()
          val record = new Record(schema)
          fieldsAndValues.foreach {
            case FieldAndValue(key, value) =>
              record.put(key, value)
          }
          record
        }
    }

  private def asAvroCompliantName(s: String): String =
    s.map(c => if (c.isLetterOrDigit || c == '_') c else '_').mkString("")
}

case class FieldAndValue(field: String, value: AnyRef)
