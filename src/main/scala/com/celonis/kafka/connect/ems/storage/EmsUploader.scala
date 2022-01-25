/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.implicits._
import com.celonis.kafka.connect.ems.config.BasicAuthentication
import com.celonis.kafka.connect.ems.config.ProxyConfig
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.storage.EmsUploader.ChunkSize
import com.celonis.kafka.connect.ems.storage.EmsUploader.buildUri
import com.typesafe.scalalogging.StrictLogging
import fs2.io.file.Files
import fs2.io.file.Flags
import fs2.io.file.Path
import org.asynchttpclient.proxy.ProxyServer
import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.asynchttpclient.Realm
import org.asynchttpclient.{ AsyncHttpClient => RawAsyncHttpClient }
import org.http4s._
import org.http4s.asynchttpclient.client.AsyncHttpClient
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.multipart.Multipart
import org.http4s.multipart.Part
import org.typelevel.ci.CIString

import java.io.File
import java.net.URL
import javax.ws.rs.core.UriBuilder
import scala.concurrent.ExecutionContext

class EmsUploader[F[_]](
  baseUrl:               URL,
  authorization:         String,
  targetTable:           String,
  connectionId:          Option[String],
  clientId:              Option[String],
  fallbackVarcharLength: Option[Int],
  primaryKeys:           Option[NonEmptyList[String]],
  ec:                    ExecutionContext,
  maybeProxyConfig:      Option[ProxyConfig],
)(
  implicit
  A: Async[F],
) extends Uploader[F]
    with Http4sClientDsl[F]
    with StrictLogging {

  val httpClient: RawAsyncHttpClient = createHttpClient()

  def createHttpClient(): RawAsyncHttpClient = {
    def createRealm(proxy: ProxyConfig): Option[Realm] =
      proxy.authentication.map {
        auth: BasicAuthentication =>
          new Realm.Builder(auth.username, auth.password)
            .setUsePreemptiveAuth(true)
            .setScheme(Realm.AuthScheme.BASIC)
            .build()
      }

    def createProxyServer: Option[ProxyServer] =
      maybeProxyConfig.map { proxy =>
        new ProxyServer.Builder(proxy.host, proxy.port).setRealm(createRealm(proxy).orNull).build()
      }

    val asyncHttpClientConfig =
      new DefaultAsyncHttpClientConfig.Builder().setProxyServer(createProxyServer.orNull).build()
    new DefaultAsyncHttpClient(asyncHttpClientConfig)
  }

  override def upload(uploadRequest: UploadRequest): F[EmsUploadResponse] = {
    val fileName =
      s"${uploadRequest.topic.value}_${uploadRequest.partition.value}_${uploadRequest.offset.value}.parquet"

    def uploadWithClient(client: Client[F]): F[EmsUploadResponse] = {
      val attributes = Vector(
        Part.fileData[F](EmsUploader.FileName,
                         fileName,
                         Files[F].readAll(Path.fromNioPath(uploadRequest.file.toPath), ChunkSize, Flags.Read),
        ),
      )
      val pks       = primaryKeys.map(nel => nel.mkString_(","))
      val multipart = Multipart[F](attributes)
      val uri       = buildUri(baseUrl, targetTable, connectionId, clientId, fallbackVarcharLength, pks)

      val request: Request[F] = Method.POST.apply(
        multipart,
        uri,
        multipart.headers.headers :+ Header.Raw(CIString("Authorization"), authorization),
      )

      for {
        _        <- A.delay(logger.info(s"Uploading URL:$uri"))
        response <- client.expectOr[EmsUploadResponse](request)(handleUploadError(_, uploadRequest))
      } yield response
    }

    AsyncHttpClient.fromClient(httpClient).use(uploadWithClient)
  }

  private def handleUploadError(response: Response[F], request: UploadRequest): F[Throwable] =
    response.status match {
      case Status.InternalServerError =>
        response.as[EmsServerErrorResponse]
          .redeemWith(
            t => unmarshalError(t, request.file, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.message, null)
              genericError(error, request.file, error.msg, response)
            },
          )
      case Status.BadRequest =>
        response.as[EmsBadRequestResponse]
          .redeemWith(
            t => unmarshalError(t, request.file, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.errors.flatMap(_.error).mkString(","), null)
              genericError(error, request.file, error.msg, response)
            },
          )

      case _ =>
        //try to parse as server error response. We don't know all the response types
        response.as[EmsServerErrorResponse]
          .redeemWith(
            t => unmarshalError(t, request.file, response),
            { msg =>
              val error = UploadFailedException(response.status, msg.message, null)
              genericError(error, request.file, error.msg, response)
            },
          )
    }

  private def genericError(throwable: Throwable, file: File, msg: String, response: Response[F]): F[Throwable] = {
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

  private def unmarshalError(throwable: Throwable, file: File, response: Response[F]): F[Throwable] = {
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
  val ChunkSize             = 8192

  def buildUri(
    base:                  URL,
    targetTable:           String,
    connectionId:          Option[String],
    clientId:              Option[String],
    fallbackVarcharLength: Option[Int],
    pks:                   Option[String],
  ): Uri = {
    val builder = connectionId.foldLeft(UriBuilder.fromUri(base.toURI)
      .queryParam(TargetTable, targetTable)) {
      case (builder, connection) => builder.queryParam(ConnectionId, connection)
    }

    clientId.foreach(builder.queryParam(ClientId, _))
    fallbackVarcharLength.foreach(v => builder.queryParam(FallbackVarcharLength, v.toString))
    pks.foreach(value => builder.queryParam(PrimaryKeys, value))
    val uri = builder.build()

    Uri.fromString(uri.toString).asInstanceOf[Right[ParseFailure, Uri]].value
  }
}
