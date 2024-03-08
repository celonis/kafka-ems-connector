package com.celonis.kafka.connect.ems.errors

import com.typesafe.scalalogging.StrictLogging

// Override handling of InvalidInput exceptions by skipping them
final class InvalidInputErrorHandler(continueOnInvalidInput: Boolean) extends StrictLogging {
  def handle(error: Throwable): Unit = error match {
    case _: InvalidInputException if continueOnInvalidInput =>
      logger.warn("Error policy is set to CONTINUE on InvalidInput", error)
    case _ => throw error
  }
}
