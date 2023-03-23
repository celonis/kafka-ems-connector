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

package com.celonis.kafka.connect.ems.storage.formats

import cats.data.NonEmptySeq
import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait Exploder {
  def explode(genericRecord: GenericRecord): NonEmptySeq[GenericRecord]

  def explodeSchema(schema: Schema): Schema
}

class NoOpExploder extends Exploder {
  override def explode(genericRecord: GenericRecord): NonEmptySeq[GenericRecord] = NonEmptySeq.one(genericRecord)

  override def explodeSchema(schema: Schema): Schema = schema
}

class ListExploder extends Exploder with LazyLogging {

  /** Given a structure that matches A GenericRecord containing a single Array containing multiple GenericRecords
    *
    * GenericRecord (type 1)
    *   - Array[]
    * -- GenericRecord (type 2)
    *
    * This flattens these out to return a Seq of GenericRecord of the second type.
    *
    * @param genericRecord
    *   original container
    * @return
    *   the flattened structure
    */
  override def explode(genericRecord: GenericRecord): NonEmptySeq[GenericRecord] = {

    val flattened: Either[String, Seq[GenericRecord]] = flattenGenericRecord(genericRecord)
    flattened match {
      case Left(exception) =>
        logger.warn(s"Not possible to explode SchemaType ${genericRecord.getSchema.getType} due to $exception")
        NonEmptySeq.one(genericRecord)
      case Right(genericRecordSeq) =>
        NonEmptySeq.fromSeqUnsafe(genericRecordSeq)
    }
  }

  def flattenGenericRecord(genericRecord: GenericRecord): Either[String, Seq[GenericRecord]] =
    validateSchema(genericRecord.getSchema) match {
      case Some(error) => error.asLeft
      case None => Option(genericRecord.get(0)).collect {
          case genericDataArray: GenericData.Array[_] => genericDataArray.asScala.toSeq
          case list:             util.List[_]         => list.asScala.toSeq
        } match {
          case None => s"Second level - Wrong type, array expected".asLeft
          case Some(elementList) =>
            val filtered = filterGenericRecords(elementList)
            if (filtered.size != elementList.size) {
              "Third level - Array contained data other than generic records".asLeft
            } else {
              filtered.asRight
            }
        }
    }

  private def validateSchema(schema: Schema): Option[String] = {
    if (Option(schema).isEmpty) {
      return Some("No schema available for genericRecord")
    }
    if (schema.getType != Schema.Type.RECORD) {
      return Some(s"Top level - Not supported for ${schema.getType}")
    }
    if (schema.getFields.size() != 1) {
      return Some(s"Second level - Too many fields. Expecting single field.")
    }
    None
  }

  private def filterGenericRecords(bArr: Seq[_]) =
    bArr.flatMap {
      case genericRecord: GenericRecord => Some(genericRecord)
      case _ => None
    }

  override def explodeSchema(schema: Schema): Schema =
    validateSchema(schema) match {
      case Some(error) =>
        logger.warn(
          s"Not possible to explode SchemaType ${schema.getType} due to $error, continuing with original schemas",
        )
        schema
      case None => schema.getField("messages").schema().getElementType
    }
}
