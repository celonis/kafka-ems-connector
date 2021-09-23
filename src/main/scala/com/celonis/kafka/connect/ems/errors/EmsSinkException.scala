/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.errors

import org.apache.kafka.connect.errors.ConnectException
import org.http4s.DecodeFailure
import org.http4s.Status

sealed trait EmsSinkException {
  def getMessage: String
}

case class UploadFailedException(status: Status, msg: String, throwable: Throwable)
    extends ConnectException(msg, throwable)
    with EmsSinkException

case class UnexpectedUploadException(msg: String, throwable: Throwable) extends ConnectException(msg, throwable)

case class UploadInvalidResponseException(failure: DecodeFailure)
    extends ConnectException(failure.getMessage(), failure)
    with EmsSinkException

case class InvalidInputException(msg: String) extends ConnectException(msg) with EmsSinkException

case class FailedObfuscationException(msg: String) extends ConnectException(msg) with EmsSinkException
