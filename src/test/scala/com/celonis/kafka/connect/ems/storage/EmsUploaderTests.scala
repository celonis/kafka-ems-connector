/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.Async
import cats.effect.Concurrent
import cats.effect.Ref
import cats.implicits._
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import fs2.Stream
import io.circe.syntax._
import org.http4s.dsl.Http4sDsl
import org.http4s.EntityDecoder.multipart
import org.http4s.HttpRoutes
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.implicits._
import org.http4s.multipart.Part
import org.http4s.server.Server
import org.http4s.Status.Forbidden
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.UUID
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext

class EmsUploaderTests extends AnyFunSuite with Matchers {
  test("uploads the file") {
    val port             = 21212
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push"
    val filePath         = UUID.randomUUID().toString + ".parquet"
    val fileContent      = Array[Byte](1, 2, 3, 4)
    val mapRef           = Ref.unsafe[IO, Map[String, Array[Byte]]](Map.empty)
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1")
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val serverResource = HttpServer.resource[IO](port, new FileService[IO](mapRef), responseQueueRef, auth, targetTable)
    val fileResource: Resource[IO, File] = Resource.make[IO, File](IO {
      val file = new File(filePath)
      file.createNewFile()
      val fw = new FileOutputStream(file)
      fw.write(fileContent)
      fw.close()
      file
    })(file => IO(file.delete()).map(_ => ()))

    (for {
      server <- serverResource
      file   <- fileResource
    } yield (server, file)).use {
      case (_, file) =>
        for {
          uploader <- IO(new EmsUploader(new URL(s"http://localhost:$port$path"),
                                         auth,
                                         targetTable,
                                         ExecutionContext.global,
          ))
          response <- uploader.upload(file)
          map      <- mapRef.get
        } yield {
          response shouldBe expectedResponse
          map(file.getName) shouldBe fileContent
        }
    }.unsafeRunSync()
  }

  test("handles invalid authorization") {
    val port             = 21212
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push"
    val filePath         = UUID.randomUUID().toString + ".parquet"
    val fileContent      = Array[Byte](1, 2, 3, 4)
    val mapRef           = Ref.unsafe[IO, Map[String, Array[Byte]]](Map.empty)
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1")
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val serverResource = HttpServer.resource[IO](port, new FileService[IO](mapRef), responseQueueRef, auth, targetTable)
    val fileResource: Resource[IO, File] = Resource.make[IO, File](IO {
      val file = new File(filePath)
      file.createNewFile()
      val fw = new FileOutputStream(file)
      fw.write(fileContent)
      fw.close()
      file
    })(file => IO(file.delete()).map(_ => ()))

    (for {
      server <- serverResource
      file   <- fileResource
    } yield (server, file)).use {
      case (_, file) =>
        for {
          uploader <- IO(new EmsUploader(new URL(s"http://localhost:$port$path"),
                                         "invalid auth",
                                         targetTable,
                                         ExecutionContext.global,
          ))
          e <- uploader.upload(file).attempt
        } yield {
          e match {
            case Left(value) =>
              value match {
                case e: UploadFailedException =>
                  e.status shouldBe Forbidden
              }
            case Right(_) => fail("Should fail with Forbidden")
          }
        }
    }
  }
}

class MultipartHttpEndpoint[F[_]: Concurrent](
  fileService: FileService[F],
  queueRef:    Ref[F, Queue[() => EmsUploadResponse]],
  auth:        String,
  targetTable: String,
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
                  val response = for {
                    (fn, queue) <- queueRef.get.map(_.dequeue)
                    _           <- queueRef.update(_ => queue)
                  } yield fn()
                  Stream.eval(response)
                }
              Ok(stream.map(_.asJson))
            case _ => BadRequest()
          }

        }
      }
  }
}

class FileService[F[_]](ref: Ref[F, Map[String, Array[Byte]]])(implicit A: Async[F]) {
  def store(part: Part[F]): Stream[F, Unit] = {
    val f = for {
      file <- A.delay(part.filename.getOrElse("sample"))
      _ <- part.body.compile.toVector.map(_.toArray).flatMap { arr =>
        ref.update(map => map + (file -> arr))
      }
    } yield ()
    Stream.eval(f)
  }
}

object HttpServer {
  def resource[F[_]](
    port:          Int,
    fileService:   FileService[F],
    responseQueue: Ref[F, Queue[() => EmsUploadResponse]],
    authorization: String,
    targetTable:   String,
  )(
    implicit
    A: Async[F],
  ): Resource[F, Server] =
    BlazeServerBuilder[F](ExecutionContext.global)(A)
      .bindLocal(port)
      .withHttpApp(new MultipartHttpEndpoint[F](fileService,
                                                responseQueue,
                                                authorization,
                                                targetTable,
      ).service.orNotFound)
      .withWebSockets(true)
      .resource
}
