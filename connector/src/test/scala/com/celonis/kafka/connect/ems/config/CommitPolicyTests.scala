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

package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CommitPolicyTests extends AnyFunSuite with Matchers {
  test(s"return an error if commit policy keys are missing") {
    val expectedMessage =
      s"Invalid [$COMMIT_SIZE_KEY]. $COMMIT_SIZE_DOC"
    EmsSinkConfig.extractCommitPolicy(Map.empty) shouldBe Left(expectedMessage)
  }

  test(s"return an error if commit policy $COMMIT_SIZE_KEY is missing") {
    val expectedMessage =
      s"Invalid [$COMMIT_SIZE_KEY]. $COMMIT_SIZE_DOC"
    EmsSinkConfig.extractCommitPolicy(Map("a" -> "b", COMMIT_SIZE_KEY + ".ext" -> 1)) shouldBe Left(
      expectedMessage,
    )
    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY       -> 1000,
                                          COMMIT_SIZE_KEY + ".ext" -> 1,
                                          COMMIT_INTERVAL_KEY      -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if commit policy $COMMIT_RECORDS_KEY is missing") {
    val expectedMessage =
      s"Invalid [$COMMIT_RECORDS_KEY]. $COMMIT_RECORDS_DOC"
    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY + ".ext" -> 1000,
                                          COMMIT_SIZE_KEY             -> 1000000,
                                          COMMIT_INTERVAL_KEY         -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $COMMIT_INTERVAL_KEY is missing") {
    val expectedMessage =
      s"Invalid [$COMMIT_INTERVAL_KEY]. $COMMIT_INTERVAL_DOC"

    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY           -> 1000,
                                          COMMIT_SIZE_KEY              -> 1000000L,
                                          COMMIT_INTERVAL_KEY + ".ext" -> 999,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $COMMIT_SIZE_KEY is less than 1mb") {
    val expectedMessage =
      s"Invalid [$COMMIT_SIZE_KEY]. Flush size needs to be at least 1000000 (1 MB)."

    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY  -> 1000,
                                          COMMIT_SIZE_KEY     -> 1L,
                                          COMMIT_INTERVAL_KEY -> 3600000,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return an error if $COMMIT_INTERVAL_KEY is less than 1000") {
    val expectedMessage =
      s"Invalid [$COMMIT_INTERVAL_KEY]. The stop gap interval for uploading the data cannot be smaller than 1000 (1s)."

    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY  -> 1000,
                                          COMMIT_SIZE_KEY     -> 1000000L,
                                          COMMIT_INTERVAL_KEY -> 999,
    )) shouldBe Left(
      expectedMessage,
    )
  }

  test(s"return the commit policy") {
    val size     = 1000001L
    val records  = 999
    val interval = 1001L
    EmsSinkConfig.extractCommitPolicy(Map(COMMIT_RECORDS_KEY  -> records,
                                          COMMIT_SIZE_KEY     -> size,
                                          COMMIT_INTERVAL_KEY -> interval,
    )) shouldBe Right(
      DefaultCommitPolicy(size, interval, records.toLong),
    )
  }
}
