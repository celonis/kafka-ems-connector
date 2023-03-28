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
import cats.effect.Concurrent
import cats.effect.IO
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.jsonOf
import org.http4s.EntityDecoder

case class EmsUploadResponse(
  id:                    String,
  fileName:              String,
  bucketId:              String,
  flushStatus:           String,
  clientId:              Option[String],
  fallbackVarcharLength: Option[Int],
  upsertStrategy:        Option[String],
)

object EmsUploadResponse {
  implicit val codec: Codec[EmsUploadResponse] = deriveCodec

  implicit def entityDecoder(
    implicit
    concurrent: Concurrent[IO]): EntityDecoder[IO, EmsUploadResponse] =
    jsonOf[IO, EmsUploadResponse]
}
