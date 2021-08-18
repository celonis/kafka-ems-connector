/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.errors
import org.http4s.DecodeFailure
import org.http4s.Status

class EmsSinkException(msg: String, throwable: Throwable) extends Exception(msg, throwable) {
  def this(msg:       String)    = this(msg, null)
  def this(throwable: Throwable) = this(throwable.getMessage, throwable)
}

class UploadFailedException(val status: Status, msg: String, throwable: Throwable)
    extends EmsSinkException(msg, throwable)
class UnexpectedUploadException(msg: String, throwable: Throwable) extends EmsSinkException(msg, throwable)
class UploadInvalidResponseException(val failure: DecodeFailure) extends EmsSinkException(failure.getMessage(), failure)
