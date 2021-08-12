/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.errors

import com.typesafe.scalalogging.StrictLogging

import java.util.Date
import enumeratum._
import enumeratum.EnumEntry.Uppercase
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException

case class ErrorTracker(
  retries:            Int,
  maxRetries:         Int,
  lastErrorMessage:   String,
  lastErrorTimestamp: Date,
  policy:             ErrorPolicy,
)

sealed trait ErrorPolicy extends EnumEntry with Uppercase {
  def handle(error: Throwable, retries: Int): Unit
}

object ErrorPolicy extends Enum[ErrorPolicy] {
  override def values: IndexedSeq[
    ErrorPolicy,
  ] = findValues

  def find(value: String): Option[ErrorPolicy] = values.find(_.entryName.toUpperCase() == value.toUpperCase())

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
}
