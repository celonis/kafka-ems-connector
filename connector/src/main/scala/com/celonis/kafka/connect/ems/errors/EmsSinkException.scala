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

package com.celonis.kafka.connect.ems.errors

import org.apache.kafka.connect.errors.ConnectException
import org.http4s.Status

sealed trait EmsSinkException {
  def getMessage: String
}

final case class UploadFailedException(status: Status, msg: String, throwable: Throwable)
    extends ConnectException(msg, throwable)
    with EmsSinkException

final case class InvalidInputException(msg: String) extends ConnectException(msg) with EmsSinkException

final case class FailedObfuscationException(msg: String) extends ConnectException(msg) with EmsSinkException
