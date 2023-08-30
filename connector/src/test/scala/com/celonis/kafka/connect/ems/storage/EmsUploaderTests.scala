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

import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import cats.syntax.option._
import com.celonis.kafka.connect.ems.config.BasicAuthentication
import com.celonis.kafka.connect.ems.config.UnproxiedHttpClientConfig
import com.celonis.kafka.connect.ems.errors.UploadFailedException
import org.http4s.Status.BadRequest
import org.http4s.Status.Forbidden
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.celonis.kafka.connect.ems.config.ProxiedHttpClientConfig
import com.celonis.kafka.connect.ems.config.PoolingConfig
import com.celonis.kafka.connect.ems.config.ProxyType

import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.UUID
import scala.collection.immutable.Queue
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CLOSE_EVERY_CONNECTION_DEFAULT_VALUE => CLOSE_CONN_DEFAULT,
}
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE => KEEPALIVE_DEFAULT,
}
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE => MAX_IDLE_DEFAULT,
}

class EmsUploaderTests extends AnyFunSuite with Matchers {

  private val defaultPoolingConfig: PoolingConfig =
    PoolingConfig(MAX_IDLE_DEFAULT, KEEPALIVE_DEFAULT, CLOSE_CONN_DEFAULT)

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

    val fileResource: Resource[IO, File] = makeFileResource(filePath, fileContent)

    (for {
      server <- serverResource
      file   <- fileResource
    } yield (server, file)).use {
      case (_, file) =>
        for {
          uploader <- IO(
            new EmsUploader[IO](
              new URL(s"http://localhost:$port$path"),
              auth,
              targetTable,
              Some("id2"),
              "CelonisKafka2Ems vx.Test",
              None,
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig),
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig).createHttpClient(),
            ),
          )
          response <- uploader.upload(UploadRequest(file.toPath, "a_filename.parquet"))
          map      <- mapRef.get
        } yield {
          response shouldBe expectedResponse
          map("a_filename.parquet") shouldBe fileContent
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

    val fileResource: Resource[IO, File] = makeFileResource(filePath, fileContent)

    (for {
      server <- serverResource
      file   <- fileResource
    } yield (server, file)).use {
      case (_, file) =>
        for {
          uploader <- IO(
            new EmsUploader[IO](
              new URL(s"http://localhost:$port$path"),
              "invalid auth",
              targetTable,
              None,
              "CelonisKafka2Ems vx.Test",
              None,
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig),
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig).createHttpClient(),
            ),
          )
          e <- uploader.upload(UploadRequest(file.toPath, "a_filename.parquet")).attempt
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
    }.unsafeRunSync()
  }

  test("handles non json responses") {
    val port             = 21212
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push/respond-with-bad-request"
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

    val fileResource: Resource[IO, File] = makeFileResource(filePath, fileContent)

    (for {
      server <- serverResource
      file   <- fileResource
    } yield (server, file)).use {
      case (_, file) =>
        for {
          uploader <- IO(
            new EmsUploader[IO](
              new URL(s"http://localhost:$port$path"),
              "this is auth",
              targetTable,
              None,
              "CelonisKafka2Ems vx.Test",
              None,
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig),
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig).createHttpClient(),
            ),
          )
          e <- uploader.upload(UploadRequest(file.toPath, "a_filename.parquet")).attempt
        } yield {
          e match {
            case Left(value) =>
              value match {
                case e: UploadFailedException =>
                  e.status shouldBe BadRequest
                  e.msg contains "Bad request string message."
              }
            case Right(_) => fail("Should fail with Forbidden")
          }
        }
    }.unsafeRunSync()
  }

  test("uploads the file through an unauthenticated proxy") {
    val proxyPort        = 21214
    val serverPort       = 21215
    val proxyAuth        = None
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push"
    val filePath         = UUID.randomUUID().toString + ".parquet"
    val fileContent      = Array[Byte](1, 2, 3, 4)
    val mapRef           = Ref.unsafe[IO, Map[String, Array[Byte]]](Map.empty)
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1".some, None, None)
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val proxyServerResource = ProxyServer.resource[IO](proxyPort, proxyAuth)
    val serverResource =
      HttpServer.resource[IO](serverPort,
                              new InMemoryFileService[IO](mapRef),
                              new QueueEmsUploadResponseProvider[IO](responseQueueRef),
                              auth,
                              targetTable,
      )

    val fileResource: Resource[IO, File] = makeFileResource(filePath, fileContent)

    (for {
      server <- serverResource
      proxy  <- proxyServerResource
      file   <- fileResource
    } yield (server, proxy, file)).use {
      case (_, _, file) =>
        for {
          uploader <- IO(
            new EmsUploader[IO](
              new URL(s"http://localhost:$serverPort$path"),
              auth,
              targetTable,
              Some("id2"),
              "CelonisKafka2Ems vx.Test",
              None,
              None,
              ProxiedHttpClientConfig(defaultPoolingConfig, "localhost", proxyPort, ProxyType.Http, None),
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig).createHttpClient(),
            ),
          )
          response <- uploader.upload(UploadRequest(file.toPath, "a_filename.parquet"))
          map      <- mapRef.get
        } yield {
          response shouldBe expectedResponse
          map("a_filename.parquet") shouldBe fileContent
        }
    }.unsafeRunSync()
  }

  test("uploads the file through an authenticated proxy") {
    val proxyPort        = 21214
    val serverPort       = 21215
    val proxyAuth        = Some(BasicAuthentication("user", "pass"))
    val auth             = "this is auth"
    val targetTable      = "tableA"
    val path             = "/api/push"
    val filePath         = UUID.randomUUID().toString + ".parquet"
    val fileContent      = Array[Byte](1, 2, 3, 4)
    val mapRef           = Ref.unsafe[IO, Map[String, Array[Byte]]](Map.empty)
    val expectedResponse = EmsUploadResponse("id1", filePath, "b1", "new", "c1".some, None, None)
    val responseQueueRef: Ref[IO, Queue[() => EmsUploadResponse]] =
      Ref.unsafe[IO, Queue[() => EmsUploadResponse]](Queue(() => expectedResponse))
    val proxyServerResource = ProxyServer.resource[IO](proxyPort, proxyAuth)
    val serverResource =
      HttpServer.resource[IO](serverPort,
                              new InMemoryFileService[IO](mapRef),
                              new QueueEmsUploadResponseProvider[IO](responseQueueRef),
                              auth,
                              targetTable,
      )
    val fileResource: Resource[IO, File] = makeFileResource(filePath, fileContent)

    (for {
      server <- serverResource
      proxy  <- proxyServerResource
      file   <- fileResource
    } yield (server, proxy, file)).use {
      case (_, _, file) =>
        for {
          uploader <- IO(
            new EmsUploader[IO](
              new URL(s"http://localhost:$serverPort$path"),
              auth,
              targetTable,
              Some("id2"),
              "CelonisKafka2Ems vx.Test",
              None,
              None,
              ProxiedHttpClientConfig(defaultPoolingConfig, "localhost", proxyPort, ProxyType.Http, proxyAuth),
              None,
              UnproxiedHttpClientConfig(defaultPoolingConfig).createHttpClient(),
            ),
          )
          response <- uploader.upload(UploadRequest(file.toPath, "a_filename.parquet"))
          map      <- mapRef.get
        } yield {
          response shouldBe expectedResponse
          map("a_filename.parquet") shouldBe fileContent
        }
    }.unsafeRunSync()
  }

  private def makeFileResource(filePath: String, fileContent: Array[Byte]): Resource[IO, File] =
    Resource.make[IO, File](IO {
      val file = new File(filePath)
      file.createNewFile()
      val fw = new FileOutputStream(file)
      fw.write(fileContent)
      fw.close()
      file
    })(file => IO(file.delete()).map(_ => ()))

}
