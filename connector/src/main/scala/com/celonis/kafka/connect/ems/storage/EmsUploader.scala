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

package com.celonis.kafka.connect.ems.storage

import cats.data.NonEmptyList
import cats.effect.Resource
import cats.effect.kernel.Async
import cats.implicits._
import com.celonis.kafka.connect.ems.config.HttpClientConfig
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.storage.EmsUploader.ChunkSize
import com.celonis.kafka.connect.ems.storage.EmsUploader.buildUri
import com.typesafe.scalalogging.StrictLogging
import fs2.io.file.Files
import fs2.io.file.Flags
import fs2.io.file.Path
import okhttp3.OkHttpClient
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.multipart.Multipart
import org.http4s.multipart.Part
import org.http4s.okhttp.client.OkHttpBuilder
import org.typelevel.ci.CIString

import java.net.URL
import javax.ws.rs.core.UriBuilder
import scala.annotation.nowarn

class EmsUploader[F[_]](
  baseUrl:               URL,
  authorization:         String,
  targetTable:           String,
  connectionId:          Option[String],
  clientId:              String,
  fallbackVarcharLength: Option[Int],
  primaryKeys:           Option[NonEmptyList[String]],
  proxyConfig:           HttpClientConfig,
  maybeOrderFieldName:   Option[String],
  okHttpClient:          OkHttpClient,
)(
  implicit
  A: Async[F],
) extends Uploader[F]
    with Http4sClientDsl[F]
    with StrictLogging {

  override def upload(uploadRequest: UploadRequest): F[EmsUploadResponse] = {

    def uploadWithClient(client: Client[F]): F[EmsUploadResponse] = {
      val attributes = Vector(
        Part.fileData[F](
          EmsUploader.FileName,
          uploadRequest.requestFilename,
          Files[F].readAll(Path.fromNioPath(uploadRequest.localFile), ChunkSize, Flags.Read),
        ),
      )
      val pks                                  = primaryKeys.map(nel => nel.mkString_(","))
      val uri                                  = buildUri(baseUrl, targetTable, connectionId, clientId, fallbackVarcharLength, pks, maybeOrderFieldName)
      @nowarn("cat=deprecation") val multipart = Multipart[F](attributes)

      val request: Request[F] = Method.POST.apply(
        multipart,
        uri,
        buildHeadersList(multipart),
      )

      for {
        _        <- A.delay(logger.info(s"Uploading URL:$uri"))
        response <- client.expectOr[EmsUploadResponse](request)(handleUploadError(_, uploadRequest))
      } yield response
    }

    createHttpClient(okHttpClient).use(uploadWithClient)
  }

  def createHttpClient(
    okHttpClient: OkHttpClient): Resource[F, Client[F]] =
    OkHttpBuilder[F](okHttpClient).resource

  private def buildHeadersList(multipart: Multipart[F]) =
    (multipart.headers.headers :+
      Header.Raw(CIString("Authorization"), authorization)) ++
      proxyConfig.authorizationHeader().map(header => Header.Raw(CIString("Proxy-Authorization"), header)).toList ++
      Option.when(proxyConfig.getPoolingConfig().closeConn)(
        Header.Raw(CIString("Connection"), "Close"),
      ).toList

  private def handleUploadError(response: Response[F], request: UploadRequest): F[Throwable] =
    response.status match {
      case Status.InternalServerError =>
        response.as[EmsServerErrorResponse]
          .redeemWith(
            t => unmarshalError(t, request.localFile, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.message, null)
              genericError(error, request.localFile, error.msg, response)
            },
          )
      case Status.BadRequest =>
        response.as[EmsBadRequestResponse]
          .redeemWith(
            t => unmarshalError(t, request.localFile, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.errors.flatMap(_.error).mkString(","), null)
              genericError(error, request.localFile, error.msg, response)
            },
          )

      case _ =>
        // try to parse as server error response. We don't know all the response types
        response.as[EmsServerErrorResponse]
          .redeemWith(
            t => unmarshalError(t, request.localFile, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.message, null)
              genericError(error, request.localFile, error.msg, response)
            },
          )
    }

  private def genericError(
    throwable: Throwable,
    file:      java.nio.file.Path,
    msg:       String,
    response:  Response[F]): F[Throwable] = {
    val error = UploadFailedException(
      response.status,
      s"Failed to upload the file:$file. Status code:${response.status.show}. $msg",
      throwable,
    )
    A.delay(
      logger.error(
        s"Failed to upload the file:$file. Status code:${response.status.show}, Error:${error.msg}",
        error,
      ),
    ).flatMap(_ => A.raiseError(error))
  }

  private def unmarshalError(throwable: Throwable, file: java.nio.file.Path, response: Response[F]): F[Throwable] = {
    val error = UploadFailedException(
      response.status,
      s"Failed to upload the file:$file. Status code:${response.status.show}. Cannot unmarshal the response.",
      throwable,
    )
    A.delay(
      logger.error(
        s"Failed to upload the file:$file. Status code:${response.status.show}, Error:${error.msg}",
        error,
      ),
    ).flatMap(_ => A.raiseError(error))

  }
}

object EmsUploader {
  val TargetTable           = "targetName"
  val ConnectionId          = "connectionId"
  val FileName              = "file"
  val ClientId              = "clientId"
  val FallbackVarcharLength = "fallbackVarcharLength"
  val PrimaryKeys           = "primaryKeys"
  val OrderFieldName        = "duplicateRemovalOrderColumn"
  val ChunkSize             = 8192

  def buildUri(
    base:                  URL,
    targetTable:           String,
    connectionId:          Option[String],
    clientId:              String,
    fallbackVarcharLength: Option[Int],
    pks:                   Option[String],
    orderableField:        Option[String]): Uri = {
    val builder = connectionId.foldLeft(UriBuilder.fromUri(base.toURI)
      .queryParam(TargetTable, targetTable)) {
      case (builder, connection) => builder.queryParam(ConnectionId, connection)
    }

    builder.queryParam(ClientId, clientId)
    fallbackVarcharLength.foreach(builder.queryParam(FallbackVarcharLength, _))
    pks.foreach(builder.queryParam(PrimaryKeys, _))
    orderableField.foreach(builder.queryParam(OrderFieldName, _))
    val uri = builder.build()

    Uri.fromString(uri.toString).asInstanceOf[Right[ParseFailure, Uri]].value
  }
}
