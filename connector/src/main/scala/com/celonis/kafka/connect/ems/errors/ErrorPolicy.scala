/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.errors

import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.config.PropertiesHelper.nonEmptyStringOr
import com.typesafe.scalalogging.StrictLogging

import enumeratum._
import enumeratum.EnumEntry.Uppercase
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException

sealed trait ErrorPolicy extends EnumEntry with Uppercase {
  def handle(error: Throwable, retries: Int): Unit
}

object ErrorPolicy extends Enum[ErrorPolicy] {
  val values: IndexedSeq[ErrorPolicy] = findValues

  case object Continue extends ErrorPolicy with StrictLogging {
    override def handle(error: Throwable, retries: Int): Unit =
      logger.warn(s"Error policy is set to continue.", error)
  }

  case object Throw extends ErrorPolicy {
    override def handle(error: Throwable, retries: Int): Unit =
      throw new ConnectException(error)
  }

  case object Retry extends ErrorPolicy with StrictLogging {
    override def handle(error: Throwable, retries: Int): Unit =
      if (retries == 0) {
        throw new ConnectException(error)
      } else {
        logger.warn(s"Error policy set to RETRY. Remaining attempts [$retries]")
        throw new RetriableException(error)
      }
  }

  def extract(props: Map[String, _]): Either[String, ErrorPolicy] =
    nonEmptyStringOr(props, ERROR_POLICY_KEY, ERROR_POLICY_DOC)
      .flatMap { constant =>
        ErrorPolicy.withNameInsensitiveOption(constant) match {
          case Some(value) => value.asRight[String]
          case None        => error(ERROR_POLICY_KEY, ERROR_POLICY_DOC)
        }
      }
}
