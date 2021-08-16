/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.UUID

class WorkingFolderTests extends AnyFunSuite with Matchers {
  test(s"return an error if $TMP_DIRECTORY_KEY is missing") {
    val expectedMessage =
      s"Invalid [$TMP_DIRECTORY_KEY]. $TMP_DIRECTORY_DOC"
    EmsSinkConfig.extractWorkingDirectory(Map.empty) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractWorkingDirectory(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractWorkingDirectory(Map("a" -> "b", TMP_DIRECTORY_KEY + ".ext" -> 1)) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $TMP_DIRECTORY_KEY is empty") {
    val expectedMessage =
      s"Invalid [$TMP_DIRECTORY_KEY]. $TMP_DIRECTORY_DOC"
    EmsSinkConfig.extractWorkingDirectory(Map(TMP_DIRECTORY_KEY -> "")) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $TMP_DIRECTORY_KEY is null") {
    val expectedMessage =
      s"Invalid [$TMP_DIRECTORY_KEY]. $TMP_DIRECTORY_DOC"
    EmsSinkConfig.extractWorkingDirectory(Map(TMP_DIRECTORY_KEY -> null)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $TMP_DIRECTORY_KEY is not a string") {
    val expectedMessage =
      s"Invalid [$TMP_DIRECTORY_KEY]. $TMP_DIRECTORY_DOC"
    EmsSinkConfig.extractWorkingDirectory(Map(TMP_DIRECTORY_KEY -> 2)) shouldBe Left(expectedMessage)
  }

  test(s"return the target table provided with $TMP_DIRECTORY_KEY") {
    val dir = new File(UUID.randomUUID().toString)
    try {
      dir.mkdir() shouldBe true
      EmsSinkConfig.extractWorkingDirectory(Map(TMP_DIRECTORY_KEY -> dir.toString)) shouldBe Right(dir.toPath)
    } finally {
      dir.delete()
      ()
    }
  }
}
