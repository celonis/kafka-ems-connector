/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.Concurrent
import cats.implicits._
import fs2.Stream
import io.circe.syntax.EncoderOps
import org.http4s.dsl.Http4sDsl
import org.http4s.EntityDecoder.multipart
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec._
import org.http4s.multipart.Part
import org.typelevel.ci.CIString

class MultipartHttpEndpoint[F[_]: Concurrent](
  fileService:      FileService[F],
  responseProvider: EmsUploadResponseProvider[F],
  auth:             String,
  targetTable:      String,
) extends Http4sDsl[F] {

  val service: HttpRoutes[F] = HttpRoutes.of {
    case req @ POST -> Root / "api" / "push" =>
      req.decodeWith(multipart[F], strict = true) { request =>
        def filterFileTypes(part: Part[F]): Boolean =
          part.headers.headers.exists(_.value.contains("filename"))

        if (!req.headers.get(CIString("Authorization")).map(_.head.value).contains(auth)) {
          Forbidden()
        } else {
          req.params.get(EmsUploader.TargetTable) match {
            case Some(value) if value == targetTable =>
              val stream = request.parts.filter(filterFileTypes).traverse(fileService.store)
                .flatMap { _ =>
                  Stream.eval(responseProvider.get)
                }
              Ok(stream.map(_.asJson))
            case _ => BadRequest()
          }

        }
      }
  }
}
