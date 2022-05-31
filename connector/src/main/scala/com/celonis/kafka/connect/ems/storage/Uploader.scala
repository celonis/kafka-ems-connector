/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import cats.Show
import cats.implicits.toShow
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic

import java.io.File
case class UploadRequest(file: File, topic: Topic, partition: Partition, offset: Offset)
object UploadRequest {
  implicit val show: Show[UploadRequest] =
    Show.show(r => s"file=${r.file} topic=${r.topic.show} partition==${r.partition.show} offset=${r.offset.show}")
}
trait Uploader[F[_]] {
  def upload(request: UploadRequest): F[EmsUploadResponse]
}
