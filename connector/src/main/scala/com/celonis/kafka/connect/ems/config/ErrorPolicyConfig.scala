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
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_CONTINUE_ON_INVALID_INPUT_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_CONTINUE_ON_INVALID_INPUT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.CONTINUE
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.RETRY
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.THROW
import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.config.PropertiesHelper.getBoolean
import com.celonis.kafka.connect.ems.config.PropertiesHelper.nonEmptyStringOr
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.ContinueOnInvalidInput

final case class ErrorPolicyConfig(
  policyType:             ErrorPolicyType,
  retryConfig:            RetryConfig,
  continueOnInvalidInput: Boolean,
) {
  lazy val errorPolicy: ErrorPolicy = {
    val innerPolicy = policyType match {
      case ErrorPolicyType.THROW    => ErrorPolicy.Throw
      case ErrorPolicyType.CONTINUE => ErrorPolicy.Continue
      case ErrorPolicyType.RETRY    => ErrorPolicy.Retry
    }
    if (continueOnInvalidInput) new ContinueOnInvalidInput(innerPolicy) else innerPolicy
  }
}

object ErrorPolicyConfig {
  sealed trait ErrorPolicyType
  object ErrorPolicyType {
    case object THROW    extends ErrorPolicyType
    case object CONTINUE extends ErrorPolicyType
    case object RETRY    extends ErrorPolicyType
  }

  def extract(props: Map[String, _]): Either[String, ErrorPolicyConfig] =
    for {
      policyType  <- extractType(props)
      retryConfig <- RetryConfig.extractRetry(props)
      continueOnInvalidInput =
        getBoolean(props, ERROR_CONTINUE_ON_INVALID_INPUT_KEY).getOrElse(ERROR_CONTINUE_ON_INVALID_INPUT_DEFAULT)
    } yield ErrorPolicyConfig(policyType, retryConfig, continueOnInvalidInput = continueOnInvalidInput)

  private def extractType(props: Map[String, _]): Either[String, ErrorPolicyType] =
    nonEmptyStringOr(props, ERROR_POLICY_KEY, ERROR_POLICY_DOC).map(_.toUpperCase)
      .flatMap {
        case "THROW"    => THROW.asRight
        case "RETRY"    => RETRY.asRight
        case "CONTINUE" => CONTINUE.asRight
        case _          => error(ERROR_POLICY_KEY, ERROR_POLICY_DOC)
      }
}
