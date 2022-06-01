/*
 * Copyright 2022 Celonis SE
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
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Paths
import java.util.UUID

class FileSystemTests extends AnyFunSuite with Matchers {
  test("test delete on flat folder") {
    val file = new File(UUID.randomUUID().toString)
    file.mkdir() shouldBe true
    FileSystem.deleteDir(file) shouldBe true
    file.exists() shouldBe false
  }

  test("test delete on nested folder") {
    val file = new File(UUID.randomUUID().toString)
    file.mkdir() shouldBe true
    val nested = Paths.get(file.toString, UUID.randomUUID().toString).toFile
    nested.mkdir() shouldBe true
    FileSystem.deleteDir(file) shouldBe true
    file.exists() shouldBe false
  }

  test("topic partition is cleaned") {
    val dir = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true

    val sink      = "abc"
    val topic     = "customers"
    val partition = 1
    Paths.get(dir.toString, sink).toFile.mkdir() shouldBe true
    Paths.get(dir.toString, sink, topic).toFile.mkdir() shouldBe true
    val topicPartitionDir = Paths.get(dir.toString, sink, topic, partition.toString)
    topicPartitionDir.toFile.mkdir() shouldBe true
    val tp = TopicPartition(new Topic(topic), new Partition(partition))
    FileSystem.cleanup(dir.toPath, sink, tp)
    topicPartitionDir.toFile.exists() shouldBe false
    FileSystem.deleteDir(dir)
    dir.exists() shouldBe false
  }

  test("create the output file") {
    val dir = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true

    val sink      = "abc"
    val topic     = "customers"
    val partition = 1
    val tp        = TopicPartition(new Topic(topic), new Partition(partition))
    val output    = FileSystem.createOutput(dir.toPath, sink, tp)
    output.close()
    output.outputFile().exists() shouldBe true
    output.outputFile() shouldBe Paths.get(dir.toString,
                                           sink,
                                           topic,
                                           partition.toString,
                                           partition.toString + ".parquet",
    ).toFile
    FileSystem.deleteDir(dir)
    dir.exists() shouldBe false
  }
}
