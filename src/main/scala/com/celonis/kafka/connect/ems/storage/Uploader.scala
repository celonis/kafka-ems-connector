/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic

import java.io.File
case class UploadRequest(file: File, topic: Topic, partition: Partition, offset: Offset)

trait Uploader[F[_]] {
  def upload(request: UploadRequest): F[EmsUploadResponse]
}
