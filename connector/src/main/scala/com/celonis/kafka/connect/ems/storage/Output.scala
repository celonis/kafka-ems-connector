package com.celonis.kafka.connect.ems.storage

import cats.effect.Async
import com.celonis.kafka.connect.ems.storage.EmsUploader.ChunkSize
import fs2.Stream
import fs2.io.file.Files
import fs2.io.file.Flags

import java.io.File

trait Output {
  def show: String

  def stream[F[_]](
    implicit
    A: Async[F],
  ): Stream[F, Byte]
}

class FileOutput(val file: File) extends Output {
  override def show: String = file.getAbsolutePath

  override def stream[F[_]](
    implicit
    A: Async[F],
  ): Stream[F, Byte] = Files[F].readAll(fs2.io.file.Path.fromNioPath(file.toPath), ChunkSize, Flags.Read)
}

class InMemoryFileOutput(val byte: Array[Byte]) extends Output {
  override def show: String = s"byte(${byte.length})"

  override def stream[F[_]](
    implicit
    A: Async[F],
  ): Stream[F, Byte] = Stream.emits(byte)
}
