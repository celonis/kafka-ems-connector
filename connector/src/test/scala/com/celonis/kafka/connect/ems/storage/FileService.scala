/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import fs2.Stream
import org.http4s.multipart.Part

trait FileService[F[_]] {
  def store(part: Part[F]): Stream[F, Unit]
}
