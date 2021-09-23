/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.sink

import cats.data.NonEmptyList
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config._
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import com.celonis.kafka.connect.ems.storage.ParquetFileCleanupRename
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
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
  test("failed obfuscation raises an exception") {
    withDir { dir =>
      val policy = DefaultCommitPolicy(1000000L, 10.seconds.toMillis, 1000)
      val task   = new EmsSinkTask()
      val sinkConfig = EmsSinkConfig(
        "sink1",
        new URL("https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
        "tableA",
        Some("id11111"),
        Some("client1212"),
        "AppKey 123",
        Retry,
        policy,
        RetryConfig(1, 1000),
        dir,
        ParquetConfig.Default,
        List("a", "b"),
        Some(512),
        Some(ObfuscationConfig(FixObfuscation(5, '*'),
                               NonEmptyList.of(ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "b")))),
        )),
      )
      val config = Map(
        ENDPOINT_KEY                -> sinkConfig.url.toString,
        TARGET_TABLE_KEY            -> sinkConfig.target,
        AUTHORIZATION_KEY           -> sinkConfig.authorizationKey,
        ERROR_POLICY_KEY            -> sinkConfig.errorPolicy.entryName,
        COMMIT_SIZE_KEY             -> policy.fileSize.toString,
        COMMIT_INTERVAL_KEY         -> policy.interval.toString,
        COMMIT_RECORDS_KEY          -> policy.records.toString,
        ERROR_RETRY_INTERVAL        -> sinkConfig.retries.interval.toString,
        NBR_OF_RETRIES_KEY          -> sinkConfig.retries.retries.toString,
        TMP_DIRECTORY_KEY           -> dir.toString,
        DEBUG_KEEP_TMP_FILES_KEY    -> (sinkConfig.parquet.cleanup == ParquetFileCleanupRename).toString,
        PARQUET_FLUSH_KEY           -> sinkConfig.parquet.rowGroupSize.toString,
        PRIMARY_KEYS_KEY            -> sinkConfig.primaryKeys.mkString(","),
        CONNECTION_ID_KEY           -> sinkConfig.connectionId.get,
        FALLBACK_VARCHAR_LENGTH_KEY -> sinkConfig.fallbackVarCharLengths.map(_.toString).orNull,
        CLIENT_ID_KEY               -> sinkConfig.clientId.orNull,
        OBFUSCATION_TYPE_KEY        -> "fix",
        OBFUSCATED_FIELDS_KEY       -> "b.x",
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
