package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType
//import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.config.PropertiesHelper.nonEmptyStringOr
import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.CONTINUE
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.RETRY
import com.celonis.kafka.connect.ems.config.ErrorPolicyConfig.ErrorPolicyType.THROW
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.ContinueOnInvalidInputPolicy

final case class ErrorPolicyConfig(
  policyType:             ErrorPolicyType,
  retryConfig:            RetryConfig,
  continueOnInvalidError: Boolean,
) {
  lazy val errorPolicy: ErrorPolicy = {
    val innerPolicy = policyType match {
      case ErrorPolicyType.THROW    => ErrorPolicy.Throw
      case ErrorPolicyType.CONTINUE => ErrorPolicy.Continue
      case ErrorPolicyType.RETRY    => ErrorPolicy.Retry
    }
    if (continueOnInvalidError) new ContinueOnInvalidInputPolicy(innerPolicy) else innerPolicy
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
    } yield ErrorPolicyConfig(policyType, retryConfig, continueOnInvalidError = false)

  private def extractType(props: Map[String, _]): Either[String, ErrorPolicyType] =
    nonEmptyStringOr(props, ERROR_POLICY_KEY, ERROR_POLICY_DOC).map(_.toUpperCase)
      .flatMap {
        case "THROW"    => THROW.asRight
        case "RETRY"    => RETRY.asRight
        case "CONTINUE" => CONTINUE.asRight
        case x          => x.asLeft
      }
}
