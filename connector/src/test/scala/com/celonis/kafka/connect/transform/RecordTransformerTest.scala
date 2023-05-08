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

package com.celonis.kafka.connect.transform

import cats.effect.unsafe.implicits._
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.celonis.kafka.connect.transform.fields.FieldInserter
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class RecordTransformerTest extends AnyFunSuite with Matchers {
  test("With Chunking enabled, heterogeneous arrays are handled properly") {
    val value = Map(
      "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
    ).asJava

    val record        = sinkRecord(value)
    val genericRecord = chunkTransform(record, 2, 20)

    genericRecord.get("payload_chunk1") shouldBe "{\"heterogeneous_arra"
    genericRecord.get("payload_chunk2") shouldBe "y\":[\"a\",1,true]}"
  }

  test("With Chunking disabled, heterogeneous arrays prevent flattening") {
    pendingUntilFixed {
      val value = Map(
        "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
      ).asJava
      val record = sinkRecord(value)
      flattenTransform(record)
      ()
    }
  }

  test("With Chunking disabled, heterogeneous arrays prevents flattening, even with discardCollection enabled") {
    pendingUntilFixed {
      val value = Map(
        "foo"                 -> "bar",
        "heterogeneous_array" -> List[Any]("a", 1, true).asJava,
      ).asJava

      val record = sinkRecord(value)
      flattenTransform(record)
      ()
    }
  }

  private def chunkTransform(record: SinkRecord, maxChunks: Int, chunkSize: Int): GenericRecord = {
    val flattenerConfig = Some(FlattenerConfig(discardCollections = false, Some(JsonBlobChunks(maxChunks, chunkSize))))
    val transformer     = RecordTransformer.fromConfig("mySink", flattenerConfig, Nil, None, FieldInserter.noop)
    transformer.transform(record).unsafeRunSync()
  }

  private def flattenTransform(record: SinkRecord, discardCollections: Boolean = false): GenericRecord = {
    val flattenerConfig = Some(FlattenerConfig(discardCollections = discardCollections, None))
    val transformer     = RecordTransformer.fromConfig("mySink", flattenerConfig, Nil, None, FieldInserter.noop)
    transformer.transform(record).unsafeRunSync()
  }

  private def sinkRecord(value: Any): SinkRecord = new SinkRecord("topic", 0, null, "aKey", null, value, 0)
}
