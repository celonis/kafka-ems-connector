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

package com.celonis.kafka.connect.ems.storage
import cats.implicits.toShow
import cats.syntax.either._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import com.celonis.kafka.connect.ems.model.RecordMetadata
import org.apache.avro.generic.GenericRecord

class PrimaryKeysValidator(pks: List[String]) {
  def validate(record: GenericRecord, metadata: RecordMetadata): Either[InvalidInputException, Unit] =
    if (pks.isEmpty) ().asRight
    else {
      val missingFields = pks.flatMap { pk =>
        Option(record.getSchema.getField(pk)) match {
          case Some(_) => None
          case None    => Some(pk)
        }
      }
      if (missingFields.nonEmpty)
        InvalidInputException(
          s"Incoming record is missing these primary key(-s):${missingFields.mkString(
              ",",
            )} for record ${metadata.show}",
        ).asLeft
      else {
        // PK values cannot be null
        val nullablePKs = pks.filter(pk => Option(record.get(pk)).isEmpty)
        if (nullablePKs.isEmpty)
          ().asRight
        else
          InvalidInputException(
            s"Incoming record cannot has null for the following primary key(-s):${nullablePKs.mkString(",")} for record ${metadata.show}",
          ).asLeft
      }
    }
}
