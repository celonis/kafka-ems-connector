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

import cats.implicits.catsSyntaxOptionId
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{CLOSE_EVERY_CONNECTION_DEFAULT_VALUE => CLOSE_CONN_DEFAULT, CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE => KEEPALIVE_DEFAULT, CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE => MAX_IDLE_DEFAULT, _}
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.storage.{FileSystemOperations, ParquetFileCleanupDelete}
import com.celonis.kafka.connect.transform.PreConversionConfig
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadataFieldInserter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

class EmsSinkConfigTest extends AnyFunSuite with Matchers {

  private val defaultPoolingConfig: PoolingConfig =
    PoolingConfig(MAX_IDLE_DEFAULT, KEEPALIVE_DEFAULT, CLOSE_CONN_DEFAULT)

  private val anEmsSinkConfig = EmsSinkConfig(
    sinkName               = "sink1",
    url                    = new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
    target                 = "tableA",
    connectionId           = Some("id222"),
    authorization          = AuthorizationHeader("AppKey 123"),
    errorPolicyConfig      = ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(10, 1000), continueOnInvalidInput = false),
    commitPolicy           = CommitPolicyConfig(1000000L, 10.seconds.toMillis, 1000),
    workingDir             = new File(UUID.randomUUID().toString).toPath,
    parquet                = ParquetConfig.default,
    primaryKeys            = List("a", "b"),
    fallbackVarCharLengths = Some(512),
    obfuscation            = None,
    http                   = UnproxiedHttpClientConfig(defaultPoolingConfig),
    explode                = ExplodeConfig.None,
    orderField             = OrderFieldConfig(EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName.some),
    preConversionConfig    = PreConversionConfig(convertDecimalsToFloat = false),
    flattenerConfig        = None,
    embedKafkaMetadata     = true,
    useInMemoryFileSystem  = false,
    allowNullsAsPks        = false,
    sinkPutTimeout         = FiniteDuration(288000L, TimeUnit.MILLISECONDS),
  )

  test(s"parse the configuration from properties") {
    parseProperties(propertiesFromConfig(anEmsSinkConfig)) shouldBe Right(anEmsSinkConfig)
  }

  test(s"parse the configuration from properties without defaults") {
    parseProperties(propertiesWithoutDefaults(propertiesFromConfig(anEmsSinkConfig))) shouldBe Right(anEmsSinkConfig)
  }

  test(s"parse PreConversionConfig") {
    val expectedWithDefault =
      anEmsSinkConfig.copy(preConversionConfig = PreConversionConfig(convertDecimalsToFloat = false))
    val properties = propertiesFromConfig(expectedWithDefault).removed(DECIMAL_CONVERSION_KEY)
    parseProperties(properties) shouldBe Right(expectedWithDefault)

    val expectedWithConversion =
      anEmsSinkConfig.copy(preConversionConfig = PreConversionConfig(convertDecimalsToFloat = true))
    val propertiesWithConversion = propertiesFromConfig(expectedWithConversion)
    parseProperties(propertiesWithConversion) shouldBe Right(expectedWithConversion)

    val expectedWithoutConversion =
      anEmsSinkConfig.copy(preConversionConfig = PreConversionConfig(convertDecimalsToFloat = false))
    val propertiesWithoutConversion = propertiesFromConfig(expectedWithoutConversion)
    parseProperties(propertiesWithoutConversion) shouldBe Right(expectedWithoutConversion)
  }

  test(s"returns an error if AUTHORIZATION_KEY is missing") {
    testMissingRequiredConfig(AUTHORIZATION_KEY)
  }

  test(s"returns an error if $TMP_DIRECTORY_KEY is missing") {
    testMissingRequiredConfig(TMP_DIRECTORY_KEY)
  }

  test(s"returns an error if $ENDPOINT_KEY is missing") {
    testMissingRequiredConfig(ENDPOINT_KEY)
  }

  test(s"returns an error if $TARGET_TABLE_KEY is missing") {
    testMissingRequiredConfig(TARGET_TABLE_KEY)
  }

  test(s"returns an error if $COMMIT_SIZE_KEY is missing") {
    testMissingRequiredConfig(COMMIT_SIZE_KEY)
  }

  test(s"returns an error if $COMMIT_RECORDS_KEY is missing") {
    testMissingRequiredConfig(COMMIT_RECORDS_KEY)
  }

  test(s"returns an error if $COMMIT_INTERVAL_KEY is missing") {
    testMissingRequiredConfig(COMMIT_INTERVAL_KEY)
  }

  test(s"returns THROW if $ERROR_POLICY_KEY is missing") {
    withMissingConfig(ERROR_POLICY_KEY) {
      case Left(_) => fail(s"Should not fail ")
      case Right(value) =>
        value.errorPolicyConfig.policyType shouldBe ErrorPolicyType.THROW
        ()
    }
  }

  test(s"returns default if $PARQUET_ROW_GROUP_SIZE_BYTES_KEY is missing") {
    withMissingConfig(PARQUET_ROW_GROUP_SIZE_BYTES_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) =>
        value.parquet.rowGroupSize shouldBe PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT
        ()
    }
  }

  test(s"returns default if $DEBUG_KEEP_TMP_FILES_KEY is missing") {
    withMissingConfig(DEBUG_KEEP_TMP_FILES_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) =>
        value.parquet.cleanup shouldBe ParquetFileCleanupDelete
        ()
    }
  }

  test(s"returns default if $PRIMARY_KEYS_KEY is missing") {
    withMissingConfig(PRIMARY_KEYS_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) =>
        value.primaryKeys shouldBe Nil
        ()
    }
  }

  test(s"returns default if $CONNECTION_ID_KEY is missing") {
    withMissingConfig(CONNECTION_ID_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) =>
        value.connectionId shouldBe None
        ()
    }
  }

  test(s"returns default if $FALLBACK_VARCHAR_LENGTH_KEY is missing") {
    withMissingConfig(FALLBACK_VARCHAR_LENGTH_KEY) {
      case Left(_) => fail("should not fail")
      case Right(value) =>
        value.fallbackVarCharLengths shouldBe None
        ()
    }
  }

  test(
    s"returns an default * obfuscation when $OBFUSCATION_TYPE_KEY is missing and $OBFUSCATED_FIELDS_KEY is present",
  ) {
    val props = propertiesFromConfig(anEmsSinkConfig).updated(OBFUSCATED_FIELDS_KEY, "a,b,c")

    withMissingConfig(props, OBFUSCATION_TYPE_KEY) {
      case Right(config) =>
        config.obfuscation.isDefined shouldBe true
        config.obfuscation.get.obfuscation shouldBe FixObfuscation(5, '*')
        ()
      case Left(_) => fail("should not fail")
    }
  }

  test("handles AppKey with quotation") {
    val expected = anEmsSinkConfig.copy(authorization = AuthorizationHeader("AppKey 123"))
    val input    = propertiesFromConfig(expected).updated("connect.ems.authorization.key", "\"AppKey 123\"")

    parseProperties(input) shouldBe Right(expected)
  }

  private def withMissingConfig(key: String)(fn: PartialFunction[Either[String, EmsSinkConfig], Unit]): Unit =
    withMissingConfig(propertiesFromConfig(anEmsSinkConfig), key)(fn)

  private def withMissingConfig(
    props: Map[String, _],
    key:   String,
  )(fn: PartialFunction[Either[String, EmsSinkConfig], Unit]): Unit =
    fn(parseProperties(props.removed(key)))

  test(s"uses the order field name specified") {
    val expected   = anEmsSinkConfig.copy(orderField = OrderFieldConfig(Some("justfortest")))
    val properties = propertiesFromConfig(expected)

    parseProperties(properties) shouldBe Right(expected)
  }

  test(s"hardcodes the working directory and forces Parquet file deletion if in memory file system is enabled") {
    val expected =
      anEmsSinkConfig.copy(workingDir = FileSystemOperations.InMemoryPseudoDir, useInMemoryFileSystem = true)

    val properties = propertiesFromConfig(expected)
      .updated(TMP_DIRECTORY_KEY, "/some/where/else") // these will be overriden
      .updated(DEBUG_KEEP_TMP_FILES_KEY, "true")

    parseProperties(properties) shouldBe Right(expected)
  }

  test("config properties not defined in config def are ignored") {
    val properties = propertiesFromConfig(anEmsSinkConfig).updated("a.non.defined.property.key", "whatever")
    parseProperties(properties) shouldBe Right(anEmsSinkConfig)
  }

  private def parseProperties(properties: Map[String, _]): Either[String, EmsSinkConfig] = {
    val parsedProps = Try(EmsSinkConfigDef.config.parse(
      properties.view.mapValues(_.toString).toMap.asJava,
    ).asScala.toMap).toEither.left.map(_.getMessage)

    parsedProps.flatMap { parsedProps =>
      EmsSinkConfig.from(properties("name").toString, parsedProps)
    }
  }

  private def propertiesFromConfig(config: EmsSinkConfig): Map[String, _] = Map(
    "name"                      -> config.sinkName,
    ENDPOINT_KEY                -> config.url.toString,
    TARGET_TABLE_KEY            -> config.target,
    AUTHORIZATION_KEY           -> config.authorization.header,
    ERROR_POLICY_KEY            -> config.errorPolicyConfig.policyType.toString,
    COMMIT_SIZE_KEY             -> config.commitPolicy.fileSize,
    COMMIT_INTERVAL_KEY         -> config.commitPolicy.interval,
    COMMIT_RECORDS_KEY          -> config.commitPolicy.records,
    ERROR_RETRY_INTERVAL        -> config.errorPolicyConfig.retryConfig.interval,
    ERROR_POLICY_RETRIES_KEY          -> config.errorPolicyConfig.retryConfig.retries,
    TMP_DIRECTORY_KEY           -> config.workingDir.toString,
    PRIMARY_KEYS_KEY            -> config.primaryKeys.mkString(","),
    CONNECTION_ID_KEY           -> config.connectionId.get,
    ORDER_FIELD_NAME_KEY        -> config.orderField.name.orNull,
    FALLBACK_VARCHAR_LENGTH_KEY -> config.fallbackVarCharLengths.orNull,
    DECIMAL_CONVERSION_KEY      -> config.preConversionConfig.convertDecimalsToFloat,
    FLATTENER_ENABLE_KEY        -> config.flattenerConfig.isDefined,
    FLATTENER_DISCARD_COLLECTIONS_KEY -> config.flattenerConfig.map(_.discardCollections).getOrElse(
      FLATTENER_DISCARD_COLLECTIONS_DEFAULT,
    ),
    USE_IN_MEMORY_FS_KEY -> config.useInMemoryFileSystem,
  )

  private def propertiesWithoutDefaults(properties: Map[String, _]): Map[String, _] =
    properties.keys.foldLeft(properties) { case (properties, key) =>
      EmsSinkConfigDef.config.configKeys().asScala.get(key) match {
        case Some(configKey) if configKey.defaultValue == properties(key) => properties.removed(key)
        case _                                                            => properties
      }
    }
  private def testMissingRequiredConfig(key: String): Unit =
    withMissingConfig(key) {
      case e =>
        e shouldBe Left(s"Missing required configuration \"$key\" which has no default value.")
        ()
    }
}
