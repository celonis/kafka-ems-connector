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
import cats.effect.Ref
import fs2.Stream
import org.http4s.multipart.Part
import cats.implicits._

class InMemoryFileService[F[_]](
  ref: Ref[F, Map[String, Array[Byte]]],
)(
  implicit
  A: Async[F],
) extends FileService[F] {
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
