/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import java.io.File
import java.nio.file.Path
import java.util.UUID

trait WorkingDirectory {
  protected def withDir[T](fn: Path => T): Unit = {
    val dir = new File(UUID.randomUUID().toString)
    dir.mkdir()
    try {
      fn(dir.toPath)
    } finally {
      FileSystem.deleteDir(dir)
      ()
    }
    ()
  }
}
