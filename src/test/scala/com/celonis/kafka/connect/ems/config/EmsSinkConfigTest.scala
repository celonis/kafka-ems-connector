/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_KEY
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URL
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
class EmsSinkConfigTest extends AnyFunSuite with Matchers {
  test(s"returns the configuration") {
    val policy = DefaultCommitPolicy(1000000L, 10.seconds, 1000)
    val dir    = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true
    try {
      val expected = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        "AppKey 123",
        Retry,
        policy,
        RetryConfig(10, 1000),
        dir.toPath,
      )

      val inputMap: Map[String, _] = Map(
        ENDPOINT_KEY         -> expected.url.toString,
        TARGET_TABLE_KEY     -> expected.target,
        AUTHORIZATION_KEY    -> expected.authorizationKey,
        ERROR_POLICY_KEY     -> expected.errorPolicy.entryName,
        COMMIT_SIZE_KEY      -> policy.fileSize,
        COMMIT_INTERVAL_KEY  -> policy.interval.toMillis,
        COMMIT_RECORDS_KEY   -> policy.records,
        ERROR_RETRY_INTERVAL -> expected.retries.interval,
        NBR_OF_RETRIES_KEY   -> expected.retries.retries,
        TMP_DIRECTORY_KEY    -> dir.toString,
      )
      EmsSinkConfig.from(
        expected.sinkName,
        inputMap,
      ) shouldBe Right(expected)

      val connectInputMap =
        EmsSinkConfigDef.config.parse(inputMap.view.mapValues(_.toString).toMap.asJava).asScala.toMap

      EmsSinkConfig.from(
        expected.sinkName,
        connectInputMap,
      ) shouldBe Right(expected)
    } finally {
      dir.delete()
      ()
    }
  }

  test(s"returns an error if $AUTHORIZATION_KEY is missing") {
    testMissingConfig(AUTHORIZATION_KEY, AUTHORIZATION_DOC)
  }

  test(s"returns an error if $TMP_DIRECTORY_KEY is missing") {
    testMissingConfig(TMP_DIRECTORY_KEY, TMP_DIRECTORY_DOC)
  }

  test(s"returns an error if $ENDPOINT_KEY is missing") {
    testMissingConfig(ENDPOINT_KEY, ENDPOINT_DOC)
  }

  test(s"returns an error if $TARGET_TABLE_KEY is missing") {
    testMissingConfig(TARGET_TABLE_KEY, TARGET_TABLE_DOC)
  }

  test(s"returns an error if $COMMIT_SIZE_KEY is missing") {
    testMissingConfig(COMMIT_SIZE_KEY, COMMIT_SIZE_DOC)
  }

  test(s"returns an error if $COMMIT_RECORDS_KEY is missing") {
    testMissingConfig(COMMIT_RECORDS_KEY, COMMIT_RECORDS_DOC)
  }

  test(s"returns an error if $COMMIT_INTERVAL_KEY is missing") {
    testMissingConfig(COMMIT_INTERVAL_KEY, COMMIT_INTERVAL_DOC)
  }

  test(s"returns an error if $ERROR_POLICY_KEY is missing") {
    testMissingConfig(ERROR_POLICY_KEY, ERROR_POLICY_DOC)
  }

  private def testMissingConfig(key: String, docs: String) = {
    val policy = DefaultCommitPolicy(1000000L, 10.seconds, 1000)
    val dir    = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true
    try {
      val sinkConfig = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        "AppKey 123",
        Retry,
        policy,
        RetryConfig(10, 1000),
        dir.toPath,
      )

      val inputMap: Map[String, _] = Map(
        ENDPOINT_KEY         -> sinkConfig.url.toString,
        TARGET_TABLE_KEY     -> sinkConfig.target,
        AUTHORIZATION_KEY    -> sinkConfig.authorizationKey,
        ERROR_POLICY_KEY     -> sinkConfig.errorPolicy.entryName,
        COMMIT_SIZE_KEY      -> policy.fileSize,
        COMMIT_INTERVAL_KEY  -> policy.interval.toMillis,
        COMMIT_RECORDS_KEY   -> policy.records,
        ERROR_RETRY_INTERVAL -> sinkConfig.retries.interval,
        NBR_OF_RETRIES_KEY   -> sinkConfig.retries.retries,
        TMP_DIRECTORY_KEY    -> dir.toString,
      ) - key
      val expected = s"Invalid [$key]. $docs"
      EmsSinkConfig.from(
        sinkConfig.sinkName,
        inputMap,
      ) shouldBe Left(expected)

      val connectInputMap =
        EmsSinkConfigDef.config.parse(inputMap.view.mapValues(_.toString).toMap.asJava).asScala.toMap

      EmsSinkConfig.from(
        sinkConfig.sinkName,
        connectInputMap,
      ) shouldBe Left(expected)
    } finally {
      dir.delete()
      ()
    }
  }
}
