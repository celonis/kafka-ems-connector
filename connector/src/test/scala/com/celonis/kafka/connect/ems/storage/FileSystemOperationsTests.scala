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
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.util.UUID

class FileSystemOperationsTests extends AnyFunSuite with Matchers {
  val fs    = Jimfs.newFileSystem(Configuration.unix())
  val fsOps = new FileSystemOperations(fs)

  test("delete a flat folder") {
    val path = fs.getPath(UUID.randomUUID().toString)
    Files.createDirectory(path)
    Files.exists(path) shouldBe true

    fsOps.deleteDir(path)
    Files.exists(path) shouldBe false
  }

  test("delete a nested folder") {
    val path = fs.getPath(UUID.randomUUID().toString)
    Files.createDirectory(path)
    val nested = fs.getPath(path.toString, UUID.randomUUID().toString)
    Files.createDirectory(nested)
    fsOps.deleteDir(path)
    Files.exists(path) shouldBe false
  }

  test("topic partition is cleaned") {

    val dir = fs.getPath(UUID.randomUUID().toString)
    Files.createDirectory(dir)

    val sink      = "abc"
    val topic     = "customers"
    val partition = 1

    Files.createDirectory(dir.resolve(sink))
    Files.createDirectory(dir.resolve(sink).resolve(topic))

    val topicPartitionDir = dir.resolve(sink).resolve(topic).resolve(partition.toString)
    Files.createDirectory(topicPartitionDir)

    val tp = TopicPartition(new Topic(topic), new Partition(partition))
    fsOps.cleanup(dir, sink, tp)

    Files.exists(topicPartitionDir) shouldBe false

    fsOps.deleteDir(dir)
    Files.exists(dir) shouldBe false
  }

  test("create the output path") {
    val dir = fs.getPath(UUID.randomUUID().toString)
    Files.createDirectory(dir)

    val sink      = "abc"
    val topic     = "customers"
    val partition = 1
    val tp        = TopicPartition(new Topic(topic), new Partition(partition))
    val output    = fsOps.createOutput(dir, sink, tp)

    output.close()
    output.outputFile() shouldBe fs.getPath(dir.toString,
                                            sink,
                                            topic,
                                            partition.toString,
                                            partition.toString + ".parquet",
    )

    fsOps.deleteDir(dir)
    Files.exists(dir) shouldBe false
  }
}
