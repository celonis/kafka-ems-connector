/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.effect.Ref
import cats.effect.kernel.Sync
import cats.implicits._

import scala.collection.immutable.Queue

trait EmsUploadResponseProvider[F[_]] {
  def get: F[EmsUploadResponse]
}

class QueueEmsUploadResponseProvider[F[_]](ref: Ref[F, Queue[() => EmsUploadResponse]])(implicit S: Sync[F])
    extends EmsUploadResponseProvider[F] {
  override def get: F[EmsUploadResponse] =
    for {
      queue                 <- ref.get
      (responseFn, newQueue) = queue.dequeue
      _                     <- ref.update(_ => newQueue)
    } yield responseFn()
}
