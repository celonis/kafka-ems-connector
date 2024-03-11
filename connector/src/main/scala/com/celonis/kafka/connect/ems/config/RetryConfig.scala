/*
 * Copyright 2024 Celonis SE
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

case class RetryConfig(retries: Int, interval: Long)

object RetryConfig {
  import EmsSinkConfigConstants._
  import PropertiesHelper._

  def extractRetry(props: Map[String, _]): Either[String, RetryConfig] =
    for {
      interval <- PropertiesHelper.getLong(props, ERROR_RETRY_INTERVAL) match {
        case Some(value) =>
          if (value < 1000) error(ERROR_RETRY_INTERVAL, "Retry interval cannot be smaller than 1000 (1s).")
          else value.asRight[String]
        case None => ERROR_RETRY_INTERVAL_DEFAULT.asRight[String]
      }
      retries <- PropertiesHelper.getInt(props, ERROR_POLICY_RETRIES_KEY) match {
        case Some(value) =>
          if (value <= 0) error(ERROR_POLICY_RETRIES_KEY, "Number of retries needs to be greater than 0.")
          else value.asRight[String]
        case None => ERROR_POLICY_RETRIES_DEFAULT.asRight[String]
      }
    } yield RetryConfig(retries, interval)

}
