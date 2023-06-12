/*
 * Copyright 2023 Celonis SE
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
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Struct

import scala.util.Try

object DataConverter {
  def apply(value: Any): Either[Throwable, GenericRecord] =
    value match {
      // We do not to deal with maps here, since they are already been converted to Structs upstream
      case struct: Struct =>
        Try(avroDataConverter.fromConnectData(struct.schema(), struct).asInstanceOf[GenericRecord]).toEither
      case other =>
        InvalidInputException(
          s"Invalid input received. To write the data to Parquet files the input needs to be an Object but found:${other.getClass.getCanonicalName}.",
        ).asLeft
    }

  private val avroDataConverter = new AvroData(100)
}
