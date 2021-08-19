/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.effect.IO
import com.celonis.kafka.connect.ems.errors.UnexpectedUploadException
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.errors.UploadInvalidResponseException
import com.celonis.kafka.connect.ems.storage.EmsUploader.buildUri
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.client.UnexpectedStatus
import org.http4s.multipart._
import org.typelevel.ci.CIString

import java.io.File
import java.net.URL
import javax.ws.rs.core.UriBuilder
import scala.concurrent.ExecutionContext

class EmsUploader(baseUrl: URL, authorization: String, targetTable: String, ec: ExecutionContext)
    extends Uploader
    with Http4sClientDsl[IO] {

  override def upload(file: File): IO[EmsUploadResponse] = {
    def uploadWithClient(client: Client[IO]): IO[EmsUploadResponse] = {

      val multipart: Multipart[IO] = Multipart[IO](
        Vector(
          Part.fileData(file.getName, file),
        ),
      )

      val uri = buildUri(baseUrl, targetTable)
      val request: Request[IO] = Method.POST(
        multipart,
        uri,
        multipart.headers.headers :+ Header.Raw(CIString("Authorization"), authorization),
      )

      client.expect[EmsUploadResponse](request).redeemWith(
        {
          case s: UnexpectedStatus =>
            IO.raiseError(UploadFailedException(s.status, s.getLocalizedMessage, s))
          case d: DecodeFailure =>
            IO.raiseError(UploadInvalidResponseException(d))
          case t =>
            IO.raiseError(UnexpectedUploadException(t.getLocalizedMessage, t))
        },
        IO(_),
      )
    }

    BlazeClientBuilder[IO](ec).resource
      .use(uploadWithClient)
  }
}

object EmsUploader {
  val TargetTable = "targetName"

  def buildUri(base: URL, targetTable: String): Uri = {
    val uri = UriBuilder.fromUri(base.toURI).queryParam("targetName", targetTable)
      .build()

    Uri.fromString(uri.toString).asInstanceOf[Right[ParseFailure, Uri]].value
  }
}
