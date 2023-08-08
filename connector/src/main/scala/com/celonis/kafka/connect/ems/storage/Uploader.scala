/*
 * Copyright 2023 Celonis SE
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

import cats.Show

import java.nio.file.Path

final case class UploadRequest(localFile: Path, requestFilename: String)

object UploadRequest {
  implicit val show: Show[UploadRequest] =
    Show.show(r => s"localFile=${r.localFile} requestFilename=${r.requestFilename}")

  def fromWriterState(state: WriterState): UploadRequest =
    UploadRequest(
      state.file,
      List(
        "topic-" + state.topicPartition.topic.value,
        "partition-" + state.topicPartition.partition.value,
        "firstOffset-" + state.firstOffset.map(_.value.toString).getOrElse("NA"),
        "lastOffset-" + state.lastOffset.value,
        "bytes-" + state.fileSize,
        "rows-" + state.records,
      ).mkString("_") + ".parquet",
    )
}

trait Uploader[F[_]] {
  def upload(request: UploadRequest): F[EmsUploadResponse]

  def getOrderFieldName: Option[String]
}
