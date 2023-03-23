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

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.Concurrent
import com.celonis.kafka.connect.ems.config.BasicAuthentication
import com.celonis.kafka.connect.ems.storage.EmsUploadResponse._
import com.typesafe.scalalogging.LazyLogging
import fs2.io.file.AccessDeniedException
import org.http4s.Header
import org.http4s.HttpRoutes
import org.http4s.Request
import org.http4s.Response
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.http4s.okhttp.client.OkHttpBuilder
import org.typelevel.ci.CIString

import java.util.Base64
import scala.util.Try
import scala.util.matching.Regex

class ProxyEndpoint[F[_]: Concurrent](
  auth: Option[BasicAuthentication],
)(
  implicit
  A: Async[F],
) extends Http4sDsl[F]
    with Http4sClientDsl[F]
    with LazyLogging {

  val service: HttpRoutes[F] = HttpRoutes.of {
    case req @ _ =>
      auth match {
        case Some(basic: BasicAuthentication) =>
          val maybeUserPassMatches = for {
            authHeaderValue <- getProxyHeader(req)
            authHeaderSplit <- splitAuth(authHeaderValue)
            decoded         <- Try(new String(Base64.getDecoder.decode(authHeaderSplit))).toOption
          } yield decoded == s"${basic.username}:${basic.password}"
          maybeUserPassMatches match {
            case Some(true)  => proxyRequest(req)
            case Some(false) => A.raiseError(new AccessDeniedException("Unauthorized"))
            case None        => A.raiseError(new AccessDeniedException("Unauthorized"))
          }
        case None => proxyRequest(req)
      }

  }

  def splitAuth(authHeaderValue: String): Option[String] = {
    val reg = new Regex("^Basic ([A-Za-z0-9]*)$", "base64gp")
    for {
      matches <- reg.findFirstMatchIn(authHeaderValue)
    } yield matches.group("base64gp")
  }

  private def proxyRequest(originalReq: Request[F]): F[Response[F]] = {

    def proxyUpload(client: Client[F]): F[EmsUploadResponse] =
      client.expectOr[EmsUploadResponse](originalReq)(handleProxyError)

    def handleProxyError(response: Response[F]): F[Throwable] =
      A.raiseError(new IllegalStateException(s"Failed ${response.status} (${response.body})"))

    val clientResponse = OkHttpBuilder.withDefaultClient.flatMap(_.resource)
      .use(proxyUpload)

    Ok(clientResponse)

  }

  private def getProxyHeader(req: Request[F]): Option[String] = {
    val proxyAuthHeader = req.headers.get(CIString("proxy-authorization"))
    proxyAuthHeader match {
      case Some(header: NonEmptyList[Header.Raw]) => Some(header.head.value)
      case None                                   => None
    }
  }
}
