/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.Async
import cats.effect.Ref
import fs2.Stream
import org.http4s.multipart.Part
import cats.implicits._

class InMemoryFileService[F[_]](ref: Ref[F, Map[String, Array[Byte]]])(implicit A: Async[F]) extends FileService[F] {
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
