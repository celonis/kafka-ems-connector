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
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URL

class EndpointTests extends AnyFunSuite with Matchers {
  test(s"return an error if $ENDPOINT_KEY is missing") {
    val expectedMessage =
      s"Invalid [$ENDPOINT_KEY]. Contains the EMS API endpoint in the form of:https://<<team>>.<<realm>>.celonis.cloud/continuous-batch-processing/api/v1/<<pool-id>>/items."
    EmsSinkConfig.extractURL(Map.empty) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractURL(Map("a" -> "b", "b" -> 1)) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractURL(Map("a" -> "b", ENDPOINT_KEY + ".ext" -> 1)) shouldBe Left(expectedMessage)
  }

  test(s"return an error if $ENDPOINT_KEY is not a valid URL") {
    val expectedMessage =
      "Invalid [connect.ems.endpoint]. Contains the EMS API endpoint in the form of:https://<<team>>.<<realm>>.celonis.cloud/continuous-batch-processing/api/v1/<<pool-id>>/items."
    EmsSinkConfig.extractURL(Map(ENDPOINT_KEY -> 1)) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractURL(Map(ENDPOINT_KEY -> "")) shouldBe Left(expectedMessage)

    EmsSinkConfig.extractURL(Map(
      ENDPOINT_KEY -> "https://<<team>>.<<realm>>.celonis.cloud/continuous-batch-processing/api/v1/<<pool-id>>/items",
    )) shouldBe Left(expectedMessage)
    EmsSinkConfig.extractURL(
      Map(ENDPOINT_KEY -> "teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
    ) shouldBe Left(expectedMessage)
  }
  test(s"return an error if $ENDPOINT_KEY is not using HTTPS protocol") {
    val expectedMessage =
      "Invalid [connect.ems.endpoint]. Contains the EMS API endpoint in the form of:https://<<team>>.<<realm>>.celonis.cloud/continuous-batch-processing/api/v1/<<pool-id>>/items."
    EmsSinkConfig.extractURL(
      Map(ENDPOINT_KEY -> "http://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"),
    ) shouldBe Left(expectedMessage)
  }

  test("return the EMS endpoint") {
    val expected = "https://teamA.realmB.celonis.cloud/continuous-batch-processing/api/v1/abc-pool/items"
    EmsSinkConfig.extractURL(
      Map(ENDPOINT_KEY -> expected),
    ) shouldBe Right(new URL(expected))
  }
}
