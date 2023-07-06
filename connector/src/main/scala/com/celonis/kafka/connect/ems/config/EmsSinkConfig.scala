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

import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.storage.FileSystemOperations
import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.PreConversionConfig
import org.apache.commons.validator.routines.UrlValidator

import java.io.File
import java.net.URL
import java.nio.file.Path

final case class EmsSinkConfig(
  sinkName:               String,
  url:                    URL,
  target:                 String,
  connectionId:           Option[String],
  authorization:          AuthorizationHeader,
  errorPolicy:            ErrorPolicy,
  commitPolicy:           CommitPolicyConfig,
  retries:                RetryConfig,
  workingDir:             Path,
  parquet:                ParquetConfig,
  primaryKeys:            List[String],
  fallbackVarCharLengths: Option[Int],
  obfuscation:            Option[ObfuscationConfig],
  http:                   HttpClientConfig,
  explode:                ExplodeConfig,
  orderField:             OrderFieldConfig,
  preConversionConfig:    PreConversionConfig,
  flattenerConfig:        Option[FlattenerConfig],
  embedKafkaMetadata:     Boolean,
  useInMemoryFileSystem:  Boolean,
  allowNullsAsPks:        Boolean,
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
      retry                 <- RetryConfig.extractRetry(props)
      useInMemoryFs          = PropertiesHelper.getBoolean(props, USE_IN_MEMORY_FS_KEY).getOrElse(USE_IN_MEMORY_FS_DEFAULT)
      allowNullsAsPks        = PropertiesHelper.getBoolean(props, NULL_PK_KEY).getOrElse(NULL_PK_KEY_DEFAULT)
      tempDir               <- if (useInMemoryFs) Right(FileSystemOperations.InMemoryPseudoDir) else extractWorkingDirectory(props)
      commitPolicy          <- CommitPolicyConfig.extract(props)
      parquetConfig         <- ParquetConfig.extract(props, useInMemoryFs, commitPolicy.fileSize)
      primaryKeys           <- extractPrimaryKeys(props)
      connectionId           = PropertiesHelper.getString(props, CONNECTION_ID_KEY).map(_.trim).filter(_.nonEmpty)
      fallbackVarCharLength <- extractFallbackVarcharLength(props)
      obfuscation           <- ObfuscationConfig.extract(props)
      explodeConfig          = ExplodeConfig.extractExplode(props)
      proxyConfig           <- HttpClientConfig.extractHttpClient(props)
      orderConfig            = OrderFieldConfig.from(props, primaryKeys)
      preConversionConfig    = PreConversionConfig.extract(props)
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
      preConversionConfig,
      flattenerConfig,
      includeEmbeddedMetadata,
      useInMemoryFs,
      allowNullsAsPks,
    )
}
