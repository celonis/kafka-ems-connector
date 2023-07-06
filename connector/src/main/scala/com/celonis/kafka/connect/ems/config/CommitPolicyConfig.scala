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
import PropertiesHelper._
import EmsSinkConfigConstants._
import cats.implicits.catsSyntaxEitherId

final case class CommitPolicyConfig(fileSize: Long, interval: Long, records: Long)

object CommitPolicyConfig {
  def extract(props: Map[String, _]): Either[String, CommitPolicyConfig] =
    for {
      size <- longOr(props, COMMIT_SIZE_KEY, COMMIT_SIZE_DOC).flatMap { l =>
        if (l < 100_000L) error(COMMIT_SIZE_KEY, "Flush size needs to be at least 100000 (100 KB).")
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
    } yield CommitPolicyConfig(size, interval, records)

}
