/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.Concurrent
import cats.effect.IO
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.http4s.circe.jsonOf
import org.http4s.EntityDecoder

case class ErrorEntry(attribute: Option[String], error: Option[String], errorCode: Option[String])
object ErrorEntry {
  implicit val codec: Codec[ErrorEntry] = deriveCodec

  implicit def entityDecoder(implicit concurrent: Concurrent[IO]): EntityDecoder[IO, ErrorEntry] =
    jsonOf[IO, ErrorEntry]
}
case class EmsBadRequestResponse(errors: List[ErrorEntry])

object EmsBadRequestResponse {
  implicit val codec: Codec[EmsBadRequestResponse] = deriveCodec

  implicit def entityDecoder(implicit concurrent: Concurrent[IO]): EntityDecoder[IO, EmsBadRequestResponse] =
    jsonOf[IO, EmsBadRequestResponse]
}

case class EmsServerErrorResponse(
  reference: String,
  message:   String,
)
object EmsServerErrorResponse {
  implicit val codec: Codec[EmsServerErrorResponse] = deriveCodec

  implicit def entityDecoder(implicit concurrent: Concurrent[IO]): EntityDecoder[IO, EmsServerErrorResponse] =
    jsonOf[IO, EmsServerErrorResponse]
}
