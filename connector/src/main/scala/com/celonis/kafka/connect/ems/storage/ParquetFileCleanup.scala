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
import com.celonis.kafka.connect.ems.model.Offset

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

sealed trait ParquetFileCleanup {
  def clean(file: File, offset: Offset): Unit
}

object ParquetFileCleanupDelete extends ParquetFileCleanup {
  override def clean(file: File, offset: Offset): Unit = {
    file.delete()
    ()
  }
}

object ParquetFileCleanupRename extends ParquetFileCleanup {
  def renamedFile(file: File, offset: Offset): Path =
    Paths.get(file.getParentFile.toPath.toString, offset.value.toString + ".parquet")
  override def clean(file: File, offset: Offset): Unit = {
    val newFile = renamedFile(file, offset)
    file.renameTo(newFile.toFile)
    ()
  }
}
