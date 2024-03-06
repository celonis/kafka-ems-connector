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
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.config.PropertiesHelper.nonEmptyPasswordOr

final case class AuthorizationHeader(header: String)

object AuthorizationHeader {
  def extract(props: Map[String, _]): Either[String, AuthorizationHeader] =
    nonEmptyPasswordOr(props, AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      .map(_.trim)
      .flatMap { auth =>
        val filtered = auth.filter(_ != '"')
        if (filtered.startsWith("AppKey") || filtered.startsWith("Bearer")) AuthorizationHeader(filtered).asRight
        else error(AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      }
}
