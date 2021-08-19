/*
 * Copyright 2017-2021 Celonis Ltd
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
