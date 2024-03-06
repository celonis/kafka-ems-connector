package com.celonis.kafka.connect.ems.config

import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.{ERROR_CONTINUE_ON_INVALID_INPUT_DEFAULT, ERROR_CONTINUE_ON_INVALID_INPUT_KEY, ERROR_POLICY_DOC, ERROR_POLICY_KEY}
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.{CONTINUE, RETRY, THROW}
import com.celonis.kafka.connect.ems.config.PropertiesHelper.{error, getBoolean, nonEmptyStringOr}
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
