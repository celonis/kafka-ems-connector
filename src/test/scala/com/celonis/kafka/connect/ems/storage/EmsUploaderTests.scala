/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.kernel.Resource
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.Ref
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import org.http4s.Status.Forbidden
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.UUID
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import cats.syntax.option._

class EmsUploaderTests extends AnyFunSuite with Matchers {
  test("uploads the file") {
    val port             = 21212
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push"
    val filePath         = UUID.randomUUID().toString + ".parquet"
    val fileContent      = Array[Byte](1, 2, 3, 4)
    val mapRef           = Ref.unsafe[IO, Map[String, Array[Byte]]](Map.empty)
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1".some, None, None)
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val serverResource =
      HttpServer.resource[IO](port,
                              new InMemoryFileService[IO](mapRef),
                              new QueueEmsUploadResponseProvider[IO](responseQueueRef),
                              auth,
                              targetTable,
      )
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
          uploader <- IO(
            new EmsUploader[IO](new URL(s"http://localhost:$port$path"),
                                auth,
                                targetTable,
                                Some("id2"),
                                None,
                                None,
                                None,
                                ExecutionContext.global,
            ),
          )
          response <- uploader.upload(UploadRequest(file, new Topic("a"), new Partition(0), new Offset(100)))
          map      <- mapRef.get
        } yield {
          response shouldBe expectedResponse
          map("a_0_100.parquet") shouldBe fileContent
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
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1".some, None, None)
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val serverResource =
      HttpServer.resource[IO](port,
                              new InMemoryFileService[IO](mapRef),
                              new QueueEmsUploadResponseProvider[IO](responseQueueRef),
                              auth,
                              targetTable,
      )
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
          uploader <- IO(
            new EmsUploader[IO](new URL(s"http://localhost:$port$path"),
                                "invalid auth",
                                targetTable,
                                None,
                                None,
                                None,
                                None,
                                ExecutionContext.global,
            ),
          )
          e <- uploader.upload(UploadRequest(file, new Topic("a"), new Partition(0), new Offset(100))).attempt
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
