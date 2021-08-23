/*
 * Copyright 2017-2021 Celonis Ltd
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
