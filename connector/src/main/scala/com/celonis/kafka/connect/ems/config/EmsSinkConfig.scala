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

import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import com.celonis.kafka.connect.transform.FlattenerConfig
import org.apache.commons.validator.routines.UrlValidator

import java.io.File
import java.net.URL
import java.nio.file.Path

case class EmsSinkConfig(
  sinkName:               String,
  url:                    URL,
  target:                 String,
  connectionId:           Option[String],
  authorization:          AuthorizationHeader,
  errorPolicy:            ErrorPolicy,
  commitPolicy:           CommitPolicy,
  retries:                RetryConfig,
  workingDir:             Path,
  parquet:                ParquetConfig,
  primaryKeys:            List[String],
  fallbackVarCharLengths: Option[Int],
  obfuscation:            Option[ObfuscationConfig],
  http:                   HttpClientConfig,
  explode:                ExplodeConfig,
  orderField:             OrderFieldConfig,
  flattenerConfig:        Option[FlattenerConfig],
  embedKafkaMetadata:     Boolean,
)

object EmsSinkConfig {
  import PropertiesHelper._

  def extractURL(props: Map[String, _]): Either[String, URL] =
    nonEmptyStringOr(props, ENDPOINT_KEY, ENDPOINT_DOC).flatMap { value =>
      if (!new UrlValidator(Array("https")).isValid(value)) error(ENDPOINT_KEY, ENDPOINT_DOC)
      else new URL(value).asRight[String]
    }

  def extractFallbackVarcharLength(props: Map[String, _]): Either[String, Option[Int]] =
    PropertiesHelper.getInt(props, FALLBACK_VARCHAR_LENGTH_KEY) match {
      case Some(value) =>
        if (value <= 0) error(FALLBACK_VARCHAR_LENGTH_KEY, FALLBACK_VARCHAR_LENGTH_DOC)
        else value.some.asRight[String]
      case None => None.asRight
    }

  def extractTargetTable(props: Map[String, _]): Either[String, String] =
    nonEmptyStringOr(props, TARGET_TABLE_KEY, TARGET_TABLE_DOC)

  def extractCommitPolicy(props: Map[String, _]): Either[String, CommitPolicy] =
    for {
      size <- longOr(props, COMMIT_SIZE_KEY, COMMIT_SIZE_DOC).flatMap { l =>
        if (l < 1000000L) error(COMMIT_SIZE_KEY, "Flush size needs to be at least 1000000 (1 MB).")
        else l.asRight[String]
      }
      records <- longOr(props, COMMIT_RECORDS_KEY, COMMIT_RECORDS_DOC).flatMap { l =>
        if (l <= 0) error(COMMIT_RECORDS_KEY, "Uploading the data to EMS requires a record count greater than 0.")
        else l.asRight[String]
      }
      interval <- longOr(props, COMMIT_INTERVAL_KEY, COMMIT_INTERVAL_DOC).flatMap { l =>
        if (l <= 1000)
          error(COMMIT_INTERVAL_KEY, "The stop gap interval for uploading the data cannot be smaller than 1000 (1s).")
        else l.asRight[String]
      }
    } yield DefaultCommitPolicy(size, interval, records)

  def extractWorkingDirectory(props: Map[String, _]): Either[String, Path] =
    nonEmptyStringOr(props, TMP_DIRECTORY_KEY, TMP_DIRECTORY_DOC)
      .flatMap { value =>
        val file = new File(value)
        if (!file.exists()) {
          if (!file.mkdir()) {
            error(TMP_DIRECTORY_KEY, s"Cannot create the folder:[$file].")
          } else file.toPath.asRight[String]
        } else {
          if (!file.isDirectory)
            error(TMP_DIRECTORY_KEY, s"Folder [$file] is pointing to a file.")
          else file.toPath.asRight[String]
        }
      }

  def extractPrimaryKeys(props: Map[String, _]): Either[String, List[String]] =
    PropertiesHelper.getString(props, PRIMARY_KEYS_KEY) match {
      case Some(value) =>
        value.split(',').map(_.trim).filter(_.nonEmpty)
          .toList
          .traverse(AvroValidation.validateName)
      case None => Nil.asRight
    }

  def from(sinkName: String, props: Map[String, _]): Either[String, EmsSinkConfig] =
    for {
      url                   <- extractURL(props)
      table                 <- extractTargetTable(props)
      authorization         <- AuthorizationHeader.extract(props)
      error                 <- ErrorPolicy.extract(props)
      commitPolicy          <- extractCommitPolicy(props)
      retry                 <- RetryConfig.extractRetry(props)
      tempDir               <- extractWorkingDirectory(props)
      parquetConfig         <- ParquetConfig.extract(props)
      primaryKeys           <- extractPrimaryKeys(props)
      connectionId           = PropertiesHelper.getString(props, CONNECTION_ID_KEY).map(_.trim).filter(_.nonEmpty)
      fallbackVarCharLength <- extractFallbackVarcharLength(props)
      obfuscation           <- ObfuscationConfig.extract(props)
      explodeConfig          = ExplodeConfig.extractExplode(props)
      proxyConfig           <- HttpClientConfig.extractHttpClient(props)
      orderConfig            = OrderFieldConfig.from(props, primaryKeys)
      flattenerConfig       <- FlattenerConfig.extract(props, fallbackVarCharLength)
      includeEmbeddedMetadata = PropertiesHelper.getBoolean(props, EMBED_KAFKA_EMBEDDED_METADATA_KEY).getOrElse(
        EMBED_KAFKA_EMBEDDED_METADATA_DEFAULT,
      )
    } yield EmsSinkConfig(
      sinkName,
      url,
      table,
      connectionId,
      authorization,
      error,
      commitPolicy,
      retry,
      tempDir,
      parquetConfig,
      primaryKeys,
      fallbackVarCharLength,
      obfuscation,
      proxyConfig,
      explodeConfig,
      orderConfig,
      flattenerConfig,
      includeEmbeddedMetadata,
    )

}
