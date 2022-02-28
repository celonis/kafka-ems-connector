/*
 * Copyright 2017-2022 Celonis Ltd
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

  implicit def entityDecoder(implicit concurrent: Concurrent[IO]): EntityDecoder[IO, EmsUploadResponse] =
    jsonOf[IO, EmsUploadResponse]
}
