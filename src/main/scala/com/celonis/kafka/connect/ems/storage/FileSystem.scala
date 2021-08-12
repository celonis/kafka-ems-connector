/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import com.celonis.kafka.connect.ems.model.TopicPartition

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.nio.file.Path
import java.nio.file.Paths

object FileSystem {
  def deleteDir(file: File): Boolean = {
    Option(file.listFiles()).foreach(_.foreach(deleteDir))
    file.delete()
  }

  def cleanup(dir: Path, sinkName: String, tp: TopicPartition): Unit = {
    //cleanup the file for the given topic and partition
    val topicPartitionDir =
      Paths.get(dir.toString, sinkName, tp.topic.value, tp.partition.value.toString)
    deleteDir(topicPartitionDir.toFile)
    ()
  }

  def createOutput(dir: Path, sinkName: String, topicPartition: TopicPartition): FileAndStream = {
    val topicPartitionDir =
      Paths.get(dir.toString, sinkName, topicPartition.topic.value, topicPartition.partition.value.toString)
    Paths.get(dir.toString, sinkName).toFile.mkdir()
    Paths.get(dir.toString, sinkName, topicPartition.topic.value).toFile.mkdir()
    Paths.get(dir.toString,
              sinkName,
              topicPartition.topic.value,
              topicPartition.partition.value.toString,
    ).toFile.mkdir()
    topicPartitionDir.toFile.createNewFile()
    val filePath             = Paths.get(topicPartitionDir.toString, topicPartition.partition.value.toString + ".parquet")
    val file                 = filePath.toFile
    val outputFile           = new FileOutputStream(file)
    val bufferedOutputStream = new BufferedOutputStream(outputFile)
    FileAndStream(bufferedOutputStream, file)
  }
}

case class FileAndStream(stream: OutputStream, file: File) extends AutoCloseable {
  override def close(): Unit = stream.close()
  def size: Long = {
    stream.flush()
    file.length()
  }
}
