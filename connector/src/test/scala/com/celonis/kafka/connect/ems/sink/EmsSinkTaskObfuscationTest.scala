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

package com.celonis.kafka.connect.ems.sink

import cats.data.NonEmptyList
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{
  CLOSE_EVERY_CONNECTION_DEFAULT_VALUE => CLOSE_CONN_DEFAULT,
  CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE => KEEPALIVE_DEFAULT,
  CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE => MAX_IDLE_DEFAULT,
  _,
}
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
import com.celonis.kafka.connect.ems.config._
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupRename
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
import com.celonis.kafka.connect.transform.PreConversionConfig
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadataFieldInserter
import com.sksamuel.avro4s.RecordFormat
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URL
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class EmsSinkTaskObfuscationTest extends AnyFunSuite with Matchers with WorkingDirectory {

  private val defaultPoolingConfig: PoolingConfig =
    PoolingConfig(MAX_IDLE_DEFAULT, KEEPALIVE_DEFAULT, CLOSE_CONN_DEFAULT)

  test("failed obfuscation raises an exception") {
    withDir { dir =>
      val policy = CommitPolicyConfig(1000000L, 10.seconds.toMillis, 1000)
      val task   = new EmsSinkTask()
      val sinkConfig = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        Some("id11111"),
        AuthorizationHeader("AppKey 123"),
        ErrorPolicyConfig(ErrorPolicyType.RETRY, RetryConfig(1, 1000), continueOnInvalidError = false),
        policy,
        dir,
        ParquetConfig.default,
        List("a", "b"),
        Some(512),
        Some(ObfuscationConfig(FixObfuscation(5, '*'),
                               NonEmptyList.of(ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "b")))),
        )),
        UnproxiedHttpClientConfig(defaultPoolingConfig),
        ExplodeConfig.None,
        OrderFieldConfig(Some(EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName)),
        PreConversionConfig(convertDecimalsToFloat = false),
        None,
        embedKafkaMetadata    = false,
        useInMemoryFileSystem = false,
        allowNullsAsPks       = false,
        sinkPutTimeout        = 4.minutes,
      )
      val config = Map(
        ENDPOINT_KEY                     -> sinkConfig.url.toString,
        TARGET_TABLE_KEY                 -> sinkConfig.target,
        AUTHORIZATION_KEY                -> sinkConfig.authorization.header,
        ERROR_POLICY_KEY                 -> sinkConfig.errorPolicyConfig.policyType.toString,
        COMMIT_SIZE_KEY                  -> policy.fileSize.toString,
        COMMIT_INTERVAL_KEY              -> policy.interval.toString,
        COMMIT_RECORDS_KEY               -> policy.records.toString,
        ERROR_RETRY_INTERVAL             -> sinkConfig.errorPolicyConfig.retryConfig.interval.toString,
        NBR_OF_RETRIES_KEY               -> sinkConfig.errorPolicyConfig.retryConfig.retries.toString,
        TMP_DIRECTORY_KEY                -> dir.toString,
        DEBUG_KEEP_TMP_FILES_KEY         -> sinkConfig.parquet.cleanup.isInstanceOf[ParquetFileCleanupRename].toString,
        PARQUET_ROW_GROUP_SIZE_BYTES_KEY -> sinkConfig.parquet.rowGroupSize.toString,
        PRIMARY_KEYS_KEY                 -> sinkConfig.primaryKeys.mkString(","),
        CONNECTION_ID_KEY                -> sinkConfig.connectionId.get,
        FALLBACK_VARCHAR_LENGTH_KEY      -> sinkConfig.fallbackVarCharLengths.map(_.toString).orNull,
        OBFUSCATION_TYPE_KEY             -> "fix",
        OBFUSCATED_FIELDS_KEY            -> "b.x",
        SINK_PUT_TIMEOUT_KEY             -> 4.minutes.toMillis.toString,
        FLATTENER_ENABLE_KEY             -> false.toString,
      )
      task.start(config.asJava)

      val avroDataConverter = new AvroData(100)
      val a                 = A("aaaa", B(1.1, "yyyy"))
      val avroRecord        = A.format.to(a)
      val schemaAndValue    = avroDataConverter.toConnectData(avroRecord.getSchema, avroRecord)
      val ex = the[ConnectException] thrownBy task.put(List(new SinkRecord("topic",
                                                                           0,
                                                                           null,
                                                                           null,
                                                                           schemaAndValue.schema(),
                                                                           schemaAndValue.value(),
                                                                           0,
      )).asJava)
      ex.getCause.getMessage shouldBe "Invalid obfuscation path: b.x. Path: b.x resolves to DOUBLE which is not allowed."
    }
  }
}

case class B(x: Double, y: String)

case class A(a: String, b: B)

object A {
  val format: RecordFormat[A] = RecordFormat[A]
}
