/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.storage
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.FileSystem
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
    output.stream.close()
    output.file.exists() shouldBe true
    output.file shouldBe Paths.get(dir.toString,
                                   sink,
                                   topic,
                                   partition.toString,
                                   partition.toString + ".parquet",
    ).toFile
    FileSystem.deleteDir(dir)
    dir.exists() shouldBe false
  }
}
