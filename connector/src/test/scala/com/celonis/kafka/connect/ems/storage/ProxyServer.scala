/*
 * Copyright 2024 Celonis SE
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
    A: Async[F]): Resource[F, Server] =
    BlazeServerBuilder[F]
      .withExecutionContext(ExecutionContext.global)
      .bindLocal(proxyPort)
      .withHttpApp(
        new ProxyEndpoint[F](
          authorization,
        ).service.orNotFound,
      )
      .resource
}
