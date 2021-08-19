/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import java.io.File
import java.nio.file.Path

trait Uploader[F[_]] {
  def upload(path: Path): F[EmsUploadResponse] = upload(path.toFile)
  def upload(file: File): F[EmsUploadResponse]
}
