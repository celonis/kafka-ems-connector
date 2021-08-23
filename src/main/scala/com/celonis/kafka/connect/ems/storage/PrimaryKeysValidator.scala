/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.syntax.either._
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import org.apache.avro.generic.GenericRecord

class PrimaryKeysValidator(pks: List[String]) {
  def validate(record: GenericRecord): Either[Throwable, Unit] =
    if (pks.isEmpty) ().asRight
    else {
      val missingFields = pks.flatMap { pk =>
        Option(record.getSchema.getField(pk)) match {
          case Some(_) => None
          case None    => Some(pk)
        }
      }
      if (missingFields.nonEmpty)
        InvalidInputException(s"Incoming record is missing these primary key(-s):${missingFields.mkString(",")}").asLeft
      else ().asRight
    }
}
