/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.errors

import com.typesafe.scalalogging.StrictLogging

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait ErrorHandler extends StrictLogging {
  var errorTracker: Option[ErrorTracker] = None

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")

  def initialize(maxRetries: Int, errorPolicy: ErrorPolicy): Unit =
    errorTracker = Some(ErrorTracker(maxRetries, maxRetries, "", new Date(), errorPolicy))

  def getErrorTrackerRetries: Int =
    errorTracker.get.retries

  def failed(): Boolean =
    errorTracker.get.retries != errorTracker.get.maxRetries

  def handleTry[A](t: Try[A]): Option[A] = {
    require(errorTracker.isDefined, "ErrorTracker is not set call. Initialize.")
    t match {
      case Success(s) =>
        //success, check if we had previous errors.
        if (errorTracker.get.retries != errorTracker.get.maxRetries) {
          logger.info(
            s"Recovered from error [${errorTracker.get.lastErrorMessage}] at " +
              s"${dateFormatter.format(errorTracker.get.lastErrorTimestamp)}",
          )
        }
        //cleared error
        resetErrorTracker()
        Some(s)
      case Failure(f) =>
        //decrement the retry count
        logger.error(s"Encountered error [${f.getMessage}]", f)
        this.errorTracker = Some(decrementErrorTracker(errorTracker.get, f.getMessage))
        handleError(f, errorTracker.get.retries, errorTracker.get.policy)
        None
    }
  }

  def resetErrorTracker(): Unit =
    errorTracker = Some(ErrorTracker(errorTracker.get.maxRetries,
                                     errorTracker.get.maxRetries,
                                     "",
                                     new Date(),
                                     errorTracker.get.policy,
    ))

  private def decrementErrorTracker(errorTracker: ErrorTracker, msg: String): ErrorTracker =
    if (errorTracker.maxRetries == -1) {
      ErrorTracker(errorTracker.retries, errorTracker.maxRetries, msg, new Date(), errorTracker.policy)
    } else {
      ErrorTracker(errorTracker.retries - 1, errorTracker.maxRetries, msg, new Date(), errorTracker.policy)
    }

  private def handleError(f: Throwable, retries: Int, policy: ErrorPolicy): Unit =
    policy.handle(f, retries)
}
