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

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETIRES_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RetryTests extends AnyFunSuite with Matchers {
  test(s"return defaults if retry keys are missing") {
    RetryConfig.extractRetry(Map.empty) shouldBe Right(RetryConfig(NBR_OF_RETIRES_DEFAULT,
                                                                   ERROR_RETRY_INTERVAL_DEFAULT,
    ))
  }

  test(s"return the retry config") {
    RetryConfig.extractRetry(Map(ERROR_RETRY_INTERVAL -> 1000, NBR_OF_RETRIES_KEY -> 10)) shouldBe Right(
      RetryConfig(
        10,
        1000,
      ),
    )
  }

  test(s"return an error if retry interval is smaller than 1s") {
    val message = s"Invalid [$ERROR_RETRY_INTERVAL]. Retry interval cannot be smaller than 1000 (1s)."
    RetryConfig.extractRetry(Map(ERROR_RETRY_INTERVAL -> 100, NBR_OF_RETRIES_KEY -> 10)) shouldBe Left(message)
  }

  test(s"return an error if retries is smaller than 1") {
    val message = s"Invalid [$NBR_OF_RETRIES_KEY]. Number of retries needs to be greater than 0."
    RetryConfig.extractRetry(Map(ERROR_RETRY_INTERVAL -> 1000, NBR_OF_RETRIES_KEY -> 0)) shouldBe Left(message)
  }
}
