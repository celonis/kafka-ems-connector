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

import cats.effect.Sync
import cats.syntax.all._
import com.celonis.kafka.connect.ems.dataplane.IngressInputMessageAvro
import com.celonis.kafka.connect.ems.dataplane.IngressInputMessageJson
import io.circe.Json
import io.confluent.connect.avro.AvroData
import io.confluent.connect.avro.AvroDataConfig
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.sink.SinkRecord

import java.util.Base64
import scala.jdk.CollectionConverters._

trait DataplaneConverter[F[_], A] {
  def convert(record: SinkRecord): F[A]
}

object DataplaneConverter {

  def toJsonMessage[F[_]](
    implicit
    F: Sync[F],
  ): F[DataplaneConverter[F, IngressInputMessageJson]] =
    F.delay(new JsonConverter())
      .flatTap(c => F.delay(c.configure(Map("schemas.enable" -> false, "converter.type" -> "key").asJava)))
      .map { jsonConverter =>
        new DataplaneConverter[F, IngressInputMessageJson] {
          override def convert(record: SinkRecord): F[IngressInputMessageJson] = {
            val toJson: AnyRef => F[Json] = {
              case s: Struct =>
                for {
                  jsonBytes <- F.delay(jsonConverter.fromConnectData("", s.schema(), s))
                  json      <- F.fromEither(io.circe.parser.parse(new String(jsonBytes)))
                } yield json
              case d =>
                F.raiseError(new IllegalArgumentException(s"Can't handle data of type ${d.getClass}"))
            }

            for {
              key   <- Option(record.key()).traverse(toJson)
              value <- Option(record.value()).traverse(toJson)
            } yield IngressInputMessageJson(key, value)

          }
        }
      }

  def toAvroMessage[F[_]](
    keySchema:   Option[Schema],
    valueSchema: Schema,
  )(
    implicit
    F: Sync[F],
  ): F[DataplaneConverter[F, IngressInputMessageAvro]] =
    F.delay(new ExposedAvroData).map { avroData =>
      new DataplaneConverter[F, IngressInputMessageAvro] {
        override def convert(record: SinkRecord): F[IngressInputMessageAvro] = {

          def connectToBase64Avro(data: AnyRef, connectSchema: ConnectSchema, avroSchema: Schema): F[String] =
            F.delay(avroData.fromConnectData(connectSchema, avroSchema, data))
              .map { // extract primitive values from wrapper container that AVRO doesn't understand
                case nr: NonRecordContainer => nr.getValue
                case other => other
              }
              .flatMap(base64Avro(_, avroSchema))

          for {
            key <-
              (Option(record.key()), Option(record.keySchema()), keySchema)
                .traverseN(connectToBase64Avro)
            value <-
              (Option(record.value()), Option(record.valueSchema()))
                .traverseN(connectToBase64Avro(_, _, valueSchema))
          } yield IngressInputMessageAvro(key, value)

        }
      }
    }

  private[conversion] def base64Avro[F[_]](
    avroData: AnyRef,
    schema:   Schema,
  )(
    implicit
    F: Sync[F],
  ): F[String] = F.delay {
    val outputStream = new ByteArrayOutputStream()
    val datumWriter  = new GenericDatumWriter[AnyRef](schema)
    val encoder      = EncoderFactory.get.binaryEncoder(outputStream, null)

    datumWriter.write(avroData, encoder)
    encoder.flush()
    Base64.getEncoder.encodeToString(outputStream.toByteArray)
  }

  private[conversion] val avroDataConfig = new AvroDataConfig.Builder()
    .`with`(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 100)
    .`with`(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true) // for enum support
    .build()

  private class ExposedAvroData extends AvroData(avroDataConfig) {
    override def fromConnectData(schema: ConnectSchema, avroSchema: Schema, value: Any): AnyRef =
      super.fromConnectData(schema, avroSchema, value)
  }
}
