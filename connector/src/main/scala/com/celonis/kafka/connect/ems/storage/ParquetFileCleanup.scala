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
import com.celonis.kafka.connect.ems.model.Offset

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

sealed trait ParquetFileCleanup {
  def clean(file: Path, offset: Offset): Unit
}

object ParquetFileCleanupDelete extends ParquetFileCleanup {
  override def clean(file: Path, offset: Offset): Unit = {
    Files.delete(file)
    ()
  }
}

object ParquetFileCleanupRename {
  val Default = new ParquetFileCleanupRename(FileSystems.getDefault)
}
class ParquetFileCleanupRename(fs: java.nio.file.FileSystem) extends ParquetFileCleanup {
  private[storage] def renamedFile(file: Path, offset: Offset): Path =
    fs.getPath(file.getParent.toString, offset.value.toString + ".parquet")
  override def clean(file: Path, offset: Offset): Unit = {
    val newFile = renamedFile(file, offset)
    Files.move(file, newFile)
    ()
  }
}
