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
import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs

import java.util.UUID

trait WorkingDirectory {

  protected implicit val fsImpl: FileSystem =
    Jimfs.newFileSystem(Configuration.unix())

  protected def withDir[T](
    fn: Path => T,
  )(
    implicit
    fs: FileSystem): Unit = {
    val dir = fs.getPath(UUID.randomUUID().toString)
    Files.createDirectory(dir)
    try {
      fn(dir)
    } finally {
      new FileSystemOperations(fs).deleteDir(dir)
      ()
    }
    ()
  }
}
