/*
 * Copyright 2017-2022 Celonis Ltd
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
