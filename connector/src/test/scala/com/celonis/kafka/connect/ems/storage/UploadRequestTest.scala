/*
 * Copyright 2024 Celonis SE
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

import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import java.time.Instant

class UploadRequestTest extends AnyFunSuite with Matchers {
  test("it builds filename from writer state") {
    val path = Paths.get("a/b")
    val state = WriterState(
      topicPartition  = TopicPartition(new Topic("aTopic"), new Partition(7)),
      firstOffset     = Some(new Offset(3)),
      lastOffset      = new Offset(4),
      committedOffset = None,
      fileSize        = 123,
      records         = 2,
      lastWriteTs     = Instant.now().toEpochMilli,
      schema          = Schema.create(Schema.Type.INT),
      file            = path,
    )

    val request = UploadRequest.fromWriterState(state)
    request.localFile shouldBe path
    request.requestFilename shouldBe s"topic-aTopic_partition-7_firstOffset-3_lastOffset-4_bytes-123_rows-2.parquet"
  }
}
