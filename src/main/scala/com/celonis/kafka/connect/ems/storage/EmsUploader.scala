/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.kernel.Async
import cats.implicits._
import com.celonis.kafka.connect.ems.errors.UnexpectedUploadException
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.errors.UploadInvalidResponseException
import com.celonis.kafka.connect.ems.storage.EmsUploader.buildUri
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.UnexpectedStatus
import org.http4s.multipart._
import org.typelevel.ci.CIString

import java.io.File
import java.net.URL
import javax.ws.rs.core.UriBuilder
import scala.concurrent.ExecutionContext

class EmsUploader[F[_]](
  baseUrl:       URL,
  authorization: String,
  targetTable:   String,
  connectionId:  Option[String],
  ec:            ExecutionContext,
)(
  implicit
  A: Async[F],
) extends Uploader[F]
    with Http4sClientDsl[F] {

  override def upload(file: File): F[EmsUploadResponse] = {
    def uploadWithClient(client: Client[F]): F[EmsUploadResponse] = {

      val multipart: Multipart[F] = Multipart[F](
        Vector(
          Part.fileData(file.getName, file),
        ),
      )

      val uri = buildUri(baseUrl, targetTable, connectionId)
      val request: Request[F] = Method.POST(
        multipart,
        uri,
        multipart.headers.headers :+ Header.Raw(CIString("Authorization"), authorization),
      )

      client.expect[EmsUploadResponse](request).redeemWith(
        {
          case s: UnexpectedStatus =>
            A.raiseError(UploadFailedException(s.status, s.getLocalizedMessage, s))
          case d: DecodeFailure =>
            A.raiseError(UploadInvalidResponseException(d))
          case t =>
            A.raiseError(UnexpectedUploadException(t.getLocalizedMessage, t))
        },
        A.delay(_),
      )
    }

    BlazeClientBuilder[F](ec).resource
      .use(uploadWithClient)
  }
}

object EmsUploader {
  val TargetTable  = "targetName"
  val ConnectionId = "connectionId"
  def buildUri(base: URL, targetTable: String, connectionId: Option[String]): Uri = {
    val uri = connectionId.foldLeft(UriBuilder.fromUri(base.toURI).queryParam(TargetTable, targetTable)) {
      case (builder, connection) => builder.queryParam(ConnectionId, connection)
    }.build()

    Uri.fromString(uri.toString).asInstanceOf[Right[ParseFailure, Uri]].value
  }
}
