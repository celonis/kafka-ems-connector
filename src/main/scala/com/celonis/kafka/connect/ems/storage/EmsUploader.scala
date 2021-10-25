/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.implicits._
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.storage.EmsUploader.ChunkSize
import com.celonis.kafka.connect.ems.storage.EmsUploader.buildUri
import com.typesafe.scalalogging.StrictLogging
import fs2.io.file.Files
import fs2.io.file.Flags
import fs2.io.file.Path
import io.circe.syntax.EncoderOps
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
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
)(
  implicit
  A: Async[F],
) extends Uploader[F]
    with Http4sClientDsl[F]
    with StrictLogging {

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
      val pks       = primaryKeys.map(nel => nel.toList.asJson.noSpaces)
      val multipart = Multipart[F](attributes)
      val uri       = buildUri(baseUrl, targetTable, connectionId, clientId, fallbackVarcharLength, pks)

      val request: Request[F] = Method.POST.apply(
        multipart,
        uri,
        multipart.headers.headers :+ Header.Raw(CIString("Authorization"), authorization),
      )

      client.expectOr[EmsUploadResponse](request)(handleUploadError(_, uploadRequest))
    }

    BlazeClientBuilder[F](ec).resource
      .use(uploadWithClient)
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
