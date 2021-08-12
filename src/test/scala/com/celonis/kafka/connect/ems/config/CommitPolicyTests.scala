/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FLUSH_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FLUSH_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FLUSH_SIZE_KEY
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class CommitPolicyTests extends AnyFunSuite with Matchers {
  test(s"return an error if commit policy keys are missing") {
    val expectedMessage =
      s"Invalid [$FLUSH_SIZE_KEY]. When accumulating the parquet files, the file is uploaded when reaching this limit."
    EmsSinkConfig.extractCommitPolicy(Map.empty) shouldBe Left(expectedMessage)
  }

  test(s"return an error if commit policy $FLUSH_SIZE_KEY is missing") {
    val expectedMessage =
      s"Invalid [$FLUSH_SIZE_KEY]. When accumulating the parquet files, the file is uploaded when reaching this limit."
    EmsSinkConfig.extractCommitPolicy(Map("a" -> "b", FLUSH_SIZE_KEY + ".ext" -> 1)) shouldBe Left(
      expectedMessage,
    )
    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY       -> 1000,
                                          FLUSH_SIZE_KEY + ".ext" -> 1,
                                          FLUSH_INTERVAL_KEY      -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if commit policy $FLUSH_RECORDS_KEY is missing") {
    val expectedMessage =
      s"Invalid [$FLUSH_RECORDS_KEY]. The number of records written to the local file before the data is uploaded to EMS."
    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY + ".ext" -> 1000,
                                          FLUSH_SIZE_KEY             -> 1000000,
                                          FLUSH_INTERVAL_KEY         -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $FLUSH_INTERVAL_KEY is missing") {
    val expectedMessage =
      s"Invalid [$FLUSH_INTERVAL_KEY]. The time interval in milliseconds to upload the data to EMS if the other two commit policies are not yet applicable."

    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY           -> 1000,
                                          FLUSH_SIZE_KEY              -> 1000000L,
                                          FLUSH_INTERVAL_KEY + ".ext" -> 999,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $FLUSH_SIZE_KEY is less than 1mb") {
    val expectedMessage =
      s"Invalid [$FLUSH_SIZE_KEY]. Flush size needs to be at least 1000000 (1 MB)."

    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY  -> 1000,
                                          FLUSH_SIZE_KEY     -> 1L,
                                          FLUSH_INTERVAL_KEY -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $FLUSH_INTERVAL_KEY is less than 1000") {
    val expectedMessage =
      s"Invalid [$FLUSH_INTERVAL_KEY]. The stop gap interval for uploading the data cannot be smaller than 1000 (1s)."

    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY  -> 1000,
                                          FLUSH_SIZE_KEY     -> 1000000L,
                                          FLUSH_INTERVAL_KEY -> 999,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return the commit policy") {
    val size     = 1000001L
    val records  = 999
    val interval = 1001L
    EmsSinkConfig.extractCommitPolicy(Map(FLUSH_RECORDS_KEY  -> records,
                                          FLUSH_SIZE_KEY     -> size,
                                          FLUSH_INTERVAL_KEY -> interval,
    )) shouldBe Right(
      DefaultCommitPolicy(size, interval.millis, records.toLong),
    )
  }
}
