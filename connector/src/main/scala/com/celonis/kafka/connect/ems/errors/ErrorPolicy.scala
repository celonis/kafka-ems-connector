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

package com.celonis.kafka.connect.ems.errors

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.{ConnectException, RetriableException}

sealed trait ErrorPolicy {
  def handle(error: Throwable, retries: Int): Unit
}

object ErrorPolicy {

  case object Continue extends ErrorPolicy with StrictLogging {
    override def handle(error: Throwable, retries: Int): Unit =
      logger.warn(s"Error policy is set to CONTINUE.", error)
  }

  case object Throw extends ErrorPolicy with StrictLogging {
    override def handle(error: Throwable, retries: Int): Unit = {
      logger.warn(s"Error policy is set to THROW.", error)
      throw new ConnectException(error)
    }
  }

  case object Retry extends ErrorPolicy with StrictLogging {
    override def handle(error: Throwable, retries: Int): Unit =
      if (retries == 0) {
        logger.warn(s"Error policy is set to RETRY and no more attempts left.", error)
        throw new ConnectException(error)
      } else {
        logger.warn(s"Error policy is set to RETRY. Remaining attempts [$retries]", error)
        throw new RetriableException(error)
      }
  }

}
