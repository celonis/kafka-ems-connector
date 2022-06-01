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
import cats.effect.Async
import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Sync
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Path
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.implicits.http4sKleisliResponseSyntaxOptionT
import org.http4s.multipart.Part
import cats.syntax.option._
import java.io.File
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.Try

object UploadServer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    (for {
      port   <- Stream.eval(IO.fromTry(Try(args.head.toInt).filter(_ >= 0).filter(_ < Short.MaxValue.toInt)))
      folder <- Stream.eval(IO.fromTry(Try(args(1))))
      auth   <- Stream.eval(IO.fromTry(Try(args(2))))
      table  <- Stream.eval(IO.fromTry(Try(args(3))))
      responseProvider = new EmsUploadResponseProvider[IO] {
        override def get: IO[EmsUploadResponse] =
          IO(EmsUploadResponse(UUID.randomUUID().toString, "fn", "b1", "new", "c1".some, None, None))
      }
      fileService = new StoredFileService[IO](Path.fromNioPath(new File(folder).toPath))
      routes      = new MultipartHttpEndpoint[IO](fileService, responseProvider, auth, table).service.orNotFound
      exitCode <- BlazeServerBuilder[IO](ExecutionContext.global)
        .bindLocal(port)
        .withHttpApp(routes)
        .serve
    } yield exitCode).compile.drain.as(ExitCode.Success)
}

trait StreamUtils[F[_]] {
  def evalF[A](thunk: => A)(implicit F:   Sync[F]): Stream[F, A]    = Stream.eval(F.delay(thunk))
  def putStrLn(value: String)(implicit F: Sync[F]): Stream[F, Unit] = evalF(println(value))
  def putStr(value:   String)(implicit F: Sync[F]): Stream[F, Unit] = evalF(print(value))
  def env(name:       String)(implicit F: Sync[F]): Stream[F, Option[String]] = evalF(sys.env.get(name))
  def error(msg:      String)(implicit F: Sync[F]): Stream[F, String] =
    Stream.raiseError(new Exception(msg)).covary[F]
}

object StreamUtils {
  implicit def syncInstance[F[_]]: StreamUtils[F] = new StreamUtils[F] {}
}

class StoredFileService[F[_]](home: Path)(implicit F: Async[F], S: StreamUtils[F]) extends FileService[F] {
  override def store(part: Part[F]): fs2.Stream[F, Unit] =
    for {
      filename <- S.evalF(part.filename.getOrElse("sample"))
      path     <- S.evalF(Path(s"$home/$filename"))
      result   <- part.body.through(Files[F].writeAll(path))
    } yield result
}
