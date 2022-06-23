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

import cats.syntax.either._
import cats.syntax.traverse._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config.PropertiesHelper._
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import org.apache.avro.Schema
import org.http4s.Uri

import java.util.UUID

final case class EmsStreamingSinkConfig(
  endpoint:      Uri,
  dataPoolId:    UUID,
  target:        String,
  keySchema:     Option[Schema],
  valueSchema:   Schema,
  authorization: AuthorizationHeader,
  errorPolicy:   ErrorPolicy,
  retries:       RetryConfig,
  obfuscation:   Option[ObfuscationConfig],
  proxy:         ProxyConfig,
  explode:       ExplodeConfig,
)

object EmsStreamingSinkConfig {

  def from(props: Map[String, _]): Either[String, EmsStreamingSinkConfig] =
    for {
      endpoint      <- extractEndpoint(props)
      dataPoolId    <- extractDataPoolId(props)
      target        <- nonEmptyStringOr(props, TARGET_TABLE_KEY, TARGET_TABLE_DOC)
      keySchema     <- extractKeySchema(props)
      valueSchema   <- extractValueSchema(props)
      authorization <- AuthorizationHeader.extract(props)
      error         <- ErrorPolicy.extract(props)
      retry         <- RetryConfig.extractRetry(props)
      obfuscation   <- ObfuscationConfig.extract(props)
      explode        = ExplodeConfig.extractExplode(props)
      proxy         <- ProxyConfig.extractProxy(props)
    } yield EmsStreamingSinkConfig(
      endpoint,
      dataPoolId,
      target,
      keySchema,
      valueSchema,
      authorization,
      error,
      retry,
      obfuscation,
      proxy,
      explode,
    )

  final case class ForTask(
    baseConfig:  EmsStreamingSinkConfig,
    targetTopic: String,
  )

  object ForTask {
    def from(props: Map[String, _]): Either[String, ForTask] =
      for {
        baseConfig <- EmsStreamingSinkConfig.from(props)
        target     <- nonEmptyStringOr(props, TARGET_TOPIC_KEY, TARGET_TOPIC_DOC)
      } yield ForTask(baseConfig, target)
  }

  def extractEndpoint(props: Map[String, _]): Either[String, Uri] =
    nonEmptyStringOr(props, ENDPOINT_KEY, ENDPOINT_DOC_STREAMING).flatMap { value =>
      Uri.fromString(value).leftMap(_.message)
    }

  def extractDataPoolId(props: Map[String, _]): Either[String, UUID] =
    nonEmptyStringOr(props, DATA_POOL_ID_KEY, DATA_POOL_ID_DOC).flatMap { value =>
      Either.catchNonFatal(UUID.fromString(value)).leftMap(_.getMessage)
    }

  def extractKeySchema(props: Map[String, _]): Either[String, Option[Schema]] =
    PropertiesHelper.getString(props, KEY_SCHEMA_KEY).traverse { value =>
      parseSchema(value)
    }

  def extractValueSchema(props: Map[String, _]): Either[String, Schema] =
    nonEmptyStringOr(props, VALUE_SCHEMA_KEY, VALUE_SCHEMA_DOC).flatMap { value =>
      parseSchema(value)
    }

  private def parseSchema(schema: String) =
    Either.catchNonFatal(new Schema.Parser().parse(schema)).leftMap(_.getMessage)

}
