/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.kernel.Resource
import cats.effect.Async
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.implicits._
import org.http4s.server.Server
import scala.concurrent.ExecutionContext

object HttpServer {
  def resource[F[_]](
    port:             Int,
    fileService:      FileService[F],
    responseProvider: EmsUploadResponseProvider[F],
    authorization:    String,
    targetTable:      String,
  )(
    implicit
    A: Async[F],
  ): Resource[F, Server] =
    BlazeServerBuilder[F](ExecutionContext.global)(A)
      .bindLocal(port)
      .withHttpApp(new MultipartHttpEndpoint[F](fileService,
                                                responseProvider,
                                                authorization,
                                                targetTable,
      ).service.orNotFound)
      .withWebSockets(true)
      .resource
}
