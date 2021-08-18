/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.IO

import java.io.File
import java.nio.file.Path

trait Uploader {
  def upload(path: Path): IO[EmsUploadResponse] = upload(path.toFile)
  def upload(file: File): IO[EmsUploadResponse]
}
