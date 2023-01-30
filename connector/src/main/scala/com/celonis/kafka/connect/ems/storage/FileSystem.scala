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

import com.celonis.kafka.connect.ems.model.TopicPartition
import com.typesafe.scalalogging.StrictLogging

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.nio.file.Path
import java.nio.file.Paths

object FileSystem extends StrictLogging {
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
    val sinkDir = Paths.get(dir.toString, sinkName).toFile
    if (!sinkDir.exists()) sinkDir.mkdir()
    val topicDir = Paths.get(dir.toString, sinkName, topicPartition.topic.value).toFile
    if (!topicDir.exists()) topicDir.mkdir()
    val topicPartitionDir =
      Paths.get(dir.toString, sinkName, topicPartition.topic.value, topicPartition.partition.value.toString).toFile
    if (!topicPartitionDir.exists()) topicPartitionDir.mkdir()
    val filePath = Paths.get(topicPartitionDir.toString, topicPartition.partition.value.toString + ".parquet")
    val file     = filePath.toFile
    if (file.exists()) {
      file.delete()
    }
    file.createNewFile()
    val outputFile           = new FileOutputStream(file)
    val bufferedOutputStream = new BufferedOutputStream(outputFile)
    new FileAndStream(bufferedOutputStream, file)
  }
}

/**
  * Wraps writing to an output. Unfortunately Parquet library hides the actual writer in [[PositionOutputStream]] implementation
  * and there's no way to get the correct data size written and the one in the buffers.
  * As a result this class updates the size it writes.
  * @param stream - Instance of the [[OutputStream]] to write the parquet data
  * @param file - The file it's writing to
  */
class FileAndStream(stream: OutputStream, file: File) extends AutoCloseable {
  private var _size:    Long = file.length()
  override def close(): Unit = stream.close()

  def write(b: Int): Unit = {
    stream.write(b)
    _size += Integer.BYTES
  }
  def write(b: Array[Byte]): Unit = {
    stream.write(b)
    _size += b.length
  }
  def write(b: Array[Byte], off: Int, len: Int): Unit = {
    stream.write(b, off, len)
    _size += len
  }

  def flush(): Unit = stream.flush()
  def size:    Long = _size

  def outputFile(): File = file
}
