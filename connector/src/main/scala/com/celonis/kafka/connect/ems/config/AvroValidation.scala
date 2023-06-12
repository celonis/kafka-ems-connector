/*
 * Copyright 2023 Celonis SE
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
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_KEY
object AvroValidation {
  def validateName(name: String): Either[String, String] = {
    val first = name.charAt(0)
    if (!(Character.isLetter(first) || first == '_')) {
      s"Invalid [$PRIMARY_KEYS_KEY]. Illegal character found for: $name. $PRIMARY_KEYS_DOC".asLeft
    } else {
      val invalidChars = name.filter(c => !(Character.isLetterOrDigit(c) || c == '_'))
      if (invalidChars.nonEmpty)
        s"Invalid [$PRIMARY_KEYS_KEY]. Illegal character found for: $name. $PRIMARY_KEYS_DOC".asLeft
      else name.asRight
    }
  }
}
