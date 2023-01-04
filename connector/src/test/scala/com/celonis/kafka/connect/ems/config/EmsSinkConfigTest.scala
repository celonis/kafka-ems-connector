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

package com.celonis.kafka.connect.ems.config

import cats.data.NonEmptyList
import cats.implicits.catsSyntaxOptionId
import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CLOSE_EVERY_CONNECTION_DEFAULT_VALUE => CLOSE_CONN_DEFAULT,
}
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE => KEEPALIVE_DEFAULT,
}
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE => MAX_IDLE_DEFAULT,
}
import com.celonis.kafka.connect.ems.conversion.NoOpOrderFieldInserter
import com.celonis.kafka.connect.ems.conversion.OrderFieldInserter
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupDelete
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupRename
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URL
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

class EmsSinkConfigTest extends AnyFunSuite with Matchers {

  private val defaultPoolingConfig: PoolingConfig =
    PoolingConfig(MAX_IDLE_DEFAULT, KEEPALIVE_DEFAULT, CLOSE_CONN_DEFAULT)

  test(s"returns the configuration") {
    val policy = DefaultCommitPolicy(1000000L, 10.seconds.toMillis, 1000)
    val dir    = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true
    try {
      val expected = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        Some("id222"),
        AuthorizationHeader("AppKey 123"),
        Retry,
        policy,
        RetryConfig(10, 1000),
        dir.toPath,
        ParquetConfig.Default,
        List("a", "b"),
        Some(512),
        None,
        UnproxiedHttpClientConfig(defaultPoolingConfig),
        ExplodeConfig.None,
        OrderFieldConfig(OrderFieldInserter.FieldName.some, OrderFieldInserter),
        None,
      )

      val inputMap: Map[String, _] = Map(
        ENDPOINT_KEY                -> expected.url.toString,
        TARGET_TABLE_KEY            -> expected.target,
        AUTHORIZATION_KEY           -> expected.authorization.header,
        ERROR_POLICY_KEY            -> expected.errorPolicy.entryName,
        COMMIT_SIZE_KEY             -> policy.fileSize,
        COMMIT_INTERVAL_KEY         -> policy.interval,
        COMMIT_RECORDS_KEY          -> policy.records,
        ERROR_RETRY_INTERVAL        -> expected.retries.interval,
        NBR_OF_RETRIES_KEY          -> expected.retries.retries,
        TMP_DIRECTORY_KEY           -> dir.toString,
        PRIMARY_KEYS_KEY            -> expected.primaryKeys.mkString(","),
        CONNECTION_ID_KEY           -> expected.connectionId.get,
        FALLBACK_VARCHAR_LENGTH_KEY -> expected.fallbackVarCharLengths.orNull,
      )
      EmsSinkConfig.from(
        expected.sinkName,
        inputMap,
      ) shouldBe Right(expected)

      val connectInputMap = {
        EmsSinkConfigDef.config.parse(inputMap.view.mapValues(_.toString).toMap.asJava).asScala.toMap
      }: @scala.annotation.nowarn("msg=Unused import")

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

  test(s"returns THROW if $ERROR_POLICY_KEY is missing") {
    withMissingConfig(ERROR_POLICY_KEY)({
      case Left(_) => fail(s"Should not fail ")
      case Right(value) =>
        value.errorPolicy shouldBe ErrorPolicy.Throw
        ()
    })
  }

  test(s"returns default if $PARQUET_FLUSH_KEY is missing") {
    withMissingConfig(PARQUET_FLUSH_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) => value.parquet.rowGroupSize shouldBe PARQUET_FLUSH_DEFAULT
        ()
    }
  }

  test(s"returns default if $DEBUG_KEEP_TMP_FILES_KEY is missing") {
    withMissingConfig(DEBUG_KEEP_TMP_FILES_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) => value.parquet.cleanup shouldBe ParquetFileCleanupDelete
        ()
    }
  }

  test(s"returns default if $PRIMARY_KEYS_KEY is missing") {
    withMissingConfig(PRIMARY_KEYS_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) => value.primaryKeys shouldBe Nil
        ()
    }
  }

  test(s"returns default if $CONNECTION_ID_KEY is missing") {
    withMissingConfig(CONNECTION_ID_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) => value.connectionId shouldBe None
        ()
    }
  }

  test(s"returns default if $FALLBACK_VARCHAR_LENGTH_KEY is missing") {
    withMissingConfig(FALLBACK_VARCHAR_LENGTH_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) => value.fallbackVarCharLengths shouldBe None
        ()
    }
  }

  test(
    s"returns an default * obfuscation when $OBFUSCATION_TYPE_KEY is missing and $OBFUSCATED_FIELDS_KEY is present",
  ) {
    withMissingConfig(OBFUSCATION_TYPE_KEY) {
      case Right(value) =>
        value.obfuscation.isDefined shouldBe true
        value.obfuscation.get.obfuscation shouldBe FixObfuscation(5, '*')
        ()
      case Left(_) => fail("should not fail")
    }
  }

  test("handles AppKey with quotation") {
    val input = Map(
      "connector.class"                         -> "com.celonis.kafka.connect.ems.sink.EmsSinkConnector",
      "connect.ems.authorization.key"           -> "\"AppKey 123\"",
      "connect.ems.retry.interval"              -> "60000",
      "tasks.max"                               -> "1",
      "topics"                                  -> "payments",
      "connect.ems.endpoint"                    -> "https://api.bamboo.cloud/continuous-batch-processing/api/v1/poolis/items",
      "connect.ems.connection.id"               -> "connectionId",
      "connect.ems.max.retries"                 -> "20",
      "connect.ems.parquet.write.flush.records" -> "5",
      "connect.ems.error.policy"                -> "RETRY",
      "value.converter.schema.registry.url"     -> "http://localhost:8081",
      "connect.ems.commit.records"              -> "5",
      "connect.ems.target.table"                -> "payments",
      "connect.ems.commit.size.bytes"           -> "1000000",
      "connect.ems.commit.interval.ms"          -> "30000",
      "connect.ems.debug.keep.parquet.files"    -> "false",
      "connect.ems.tmp.dir"                     -> "/tmp/ems",
      "name"                                    -> "kafka2ems",
      "value.converter"                         -> "io.confluent.connect.avro.AvroConverter",
      "key.converter"                           -> "org.apache.kafka.connect.storage.StringConverter",
    )
    EmsSinkConfig.from("tst", EmsSinkConfigDef.config.parse(input.asJava).asScala.toMap) match {
      case Left(value) => throw new ConnectException(value)
      case Right(value) =>
        value.authorization.header shouldBe "AppKey 123"
    }
  }

  private def withMissingConfig(key: String)(fn: PartialFunction[Either[String, EmsSinkConfig], Unit]): Unit = {
    val policy = DefaultCommitPolicy(1000000L, 10.seconds.toMillis, 1000)
    val dir    = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true
    try {
      val sinkConfig = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        Some("id11111"),
        AuthorizationHeader("AppKey 123"),
        Retry,
        policy,
        RetryConfig(10, 1000),
        dir.toPath,
        ParquetConfig.Default,
        List("a", "b"),
        Some(512),
        Some(ObfuscationConfig(FixObfuscation(5, '*'),
                               NonEmptyList.of(ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "b")))),
        )),
        UnproxiedHttpClientConfig(defaultPoolingConfig),
        ExplodeConfig.None,
        OrderFieldConfig(OrderFieldInserter.FieldName.some, OrderFieldInserter),
        None,
      )

      val inputMap: Map[String, _] = Map(
        ENDPOINT_KEY                -> sinkConfig.url.toString,
        TARGET_TABLE_KEY            -> sinkConfig.target,
        AUTHORIZATION_KEY           -> sinkConfig.authorization.header,
        ERROR_POLICY_KEY            -> sinkConfig.errorPolicy.entryName,
        COMMIT_SIZE_KEY             -> policy.fileSize,
        COMMIT_INTERVAL_KEY         -> policy.interval,
        COMMIT_RECORDS_KEY          -> policy.records,
        ERROR_RETRY_INTERVAL        -> sinkConfig.retries.interval,
        NBR_OF_RETRIES_KEY          -> sinkConfig.retries.retries,
        TMP_DIRECTORY_KEY           -> dir.toString,
        DEBUG_KEEP_TMP_FILES_KEY    -> (sinkConfig.parquet.cleanup == ParquetFileCleanupRename),
        PARQUET_FLUSH_KEY           -> sinkConfig.parquet.rowGroupSize,
        PRIMARY_KEYS_KEY            -> sinkConfig.primaryKeys.mkString(","),
        CONNECTION_ID_KEY           -> sinkConfig.connectionId.get,
        FALLBACK_VARCHAR_LENGTH_KEY -> sinkConfig.fallbackVarCharLengths.map(_.toString).orNull,
        OBFUSCATION_TYPE_KEY        -> "fix",
        OBFUSCATED_FIELDS_KEY       -> "a.b",
      ) - key

      (Try {
        EmsSinkConfigDef.config.parse(inputMap.view.mapValues(_.toString).toMap.asJava).asScala.toMap
      }: @scala.annotation.nowarn("msg=Unused import")).toEither.leftMap(_.getMessage)
        .foreach { connectInputMap =>
          fn(EmsSinkConfig.from(
            sinkConfig.sinkName,
            connectInputMap,
          ))
        }
    } finally {
      dir.delete()
      ()
    }
  }

  test(s"uses the order field name specified") {
    val policy = DefaultCommitPolicy(1000000L, 10.seconds.toMillis, 1000)
    val dir    = new File(UUID.randomUUID().toString)
    dir.mkdir() shouldBe true
    val orderFieldName = "justfortest"
    try {
      val expected = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        Some("id222"),
        AuthorizationHeader("AppKey 123"),
        Retry,
        policy,
        RetryConfig(10, 1000),
        dir.toPath,
        ParquetConfig.Default,
        List("a", "b"),
        Some(512),
        None,
        UnproxiedHttpClientConfig(defaultPoolingConfig),
        ExplodeConfig.None,
        OrderFieldConfig(orderFieldName.some, NoOpOrderFieldInserter),
        None,
      )

      val inputMap: Map[String, _] = Map(
        ENDPOINT_KEY                -> expected.url.toString,
        TARGET_TABLE_KEY            -> expected.target,
        AUTHORIZATION_KEY           -> expected.authorization.header,
        ERROR_POLICY_KEY            -> expected.errorPolicy.entryName,
        COMMIT_SIZE_KEY             -> policy.fileSize,
        COMMIT_INTERVAL_KEY         -> policy.interval,
        COMMIT_RECORDS_KEY          -> policy.records,
        ERROR_RETRY_INTERVAL        -> expected.retries.interval,
        NBR_OF_RETRIES_KEY          -> expected.retries.retries,
        TMP_DIRECTORY_KEY           -> dir.toString,
        PRIMARY_KEYS_KEY            -> expected.primaryKeys.mkString(","),
        CONNECTION_ID_KEY           -> expected.connectionId.get,
        FALLBACK_VARCHAR_LENGTH_KEY -> expected.fallbackVarCharLengths.orNull,
        ORDER_FIELD_NAME_KEY        -> orderFieldName,
      )
      EmsSinkConfig.from(
        expected.sinkName,
        inputMap,
      ) shouldBe Right(expected)

      val connectInputMap = {
        EmsSinkConfigDef.config.parse(inputMap.view.mapValues(_.toString).toMap.asJava).asScala.toMap
      }: @scala.annotation.nowarn("msg=Unused import")

      EmsSinkConfig.from(
        expected.sinkName,
        connectInputMap,
      ) shouldBe Right(expected)
    } finally {
      dir.delete()
      ()
    }
  }

  private def testMissingConfig(key: String, docs: String): Unit =
    withMissingConfig(key) {
      case e =>
        e shouldBe Left(s"Invalid [$key]. $docs")
        ()
    }
}
