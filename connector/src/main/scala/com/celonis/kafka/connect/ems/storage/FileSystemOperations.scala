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
import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import com.typesafe.scalalogging.StrictLogging

import java.io.BufferedOutputStream
import java.io.OutputStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

object FileSystemOperations {
  private val jimfs = Jimfs.newFileSystem(Configuration.unix())

  lazy val InMemoryPseudoDir = {
    val path = jimfs.getPath("/in-mem/pseudo/dir")
    Files.createDirectories(path)
    path
  }

  lazy val Default:  FileSystemOperations = new FileSystemOperations(FileSystems.getDefault)
  lazy val InMemory: FileSystemOperations = new FileSystemOperations(jimfs)
}

class FileSystemOperations(fs: java.nio.file.FileSystem) extends StrictLogging {

  def deleteDir(dir: Path): Unit = {
    if (Files.isDirectory(dir))
      Files.list(dir).forEach(deleteDir(_))

    if (Files.exists(dir)) Files.delete(dir)
  }

  def cleanup(dir: Path, sinkName: String, tp: TopicPartition): Unit = {
    //cleanup the file for the given topic and partition
    val topicPartitionDir =
      fs.getPath(dir.toString, sinkName, tp.topic.value, tp.partition.value.toString)
    deleteDir(topicPartitionDir)
    ()
  }

  def createOutput(dir: Path, sinkName: String, topicPartition: TopicPartition): FileAndStream = {
    val sinkDir = fs.getPath(dir.toString, sinkName)
    if (!Files.exists(sinkDir)) Files.createDirectory(sinkDir)

    val topicDir = fs.getPath(dir.toString, sinkName, topicPartition.topic.value)
    if (!Files.exists(topicDir)) Files.createDirectory(topicDir)

    val topicPartitionDir =
      fs.getPath(dir.toString, sinkName, topicPartition.topic.value, topicPartition.partition.value.toString)
    if (!Files.exists(topicPartitionDir)) Files.createDirectory(topicPartitionDir)

    val filePath = fs.getPath(topicPartitionDir.toString, topicPartition.partition.value.toString + ".parquet")
    if (Files.exists(filePath)) {
      Files.delete(filePath)
    }
    val outputStream         = Files.newOutputStream(filePath)
    val bufferedOutputStream = new BufferedOutputStream(outputStream)
    new FileAndStream(bufferedOutputStream, filePath)
  }
}

/**
  * Wraps writing to an output. Unfortunately Parquet library hides the actual writer in [[PositionOutputStream]] implementation
  * and there's no way to get the correct data size written and the one in the buffers.
  * As a result this class updates the size it writes.
  * @param stream - Instance of the [[OutputStream]] to write the parquet data
  * @param file - The file it's writing to
  */
class FileAndStream(stream: OutputStream, file: Path) extends AutoCloseable {
  private var _size:    Long = 0
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

  def outputFile(): Path = file
}
