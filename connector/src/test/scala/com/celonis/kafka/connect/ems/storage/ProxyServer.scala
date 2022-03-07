/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.Async
import cats.effect.kernel.Resource
import com.celonis.kafka.connect.ems.config.BasicAuthentication
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Server

import scala.concurrent.ExecutionContext

object ProxyServer {
  def resource[F[_]](
    proxyPort:     Int,
    authorization: Option[BasicAuthentication],
  )(
    implicit
    A: Async[F],
  ): Resource[F, Server] =
    BlazeServerBuilder[F](ExecutionContext.global)(A)
      .bindLocal(proxyPort)
      .withHttpApp(
        new ProxyEndpoint[F](
          authorization,
        ).service.orNotFound,
      )
      .withWebSockets(true)
      .resource
}
