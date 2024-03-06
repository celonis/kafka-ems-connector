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

package com.celonis.kafka.connect.ems.model

import org.apache.commons.codec.digest.DigestUtils

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

sealed trait DataObfuscation {
  def obfuscate(value: String): String
}

object DataObfuscation {

  case class FixObfuscation(chars: Int, char: Char) extends DataObfuscation {
    val constant: String = Array.fill(chars)(char).mkString

    override def obfuscate(value: String): String = if (value == null) null else constant
  }

  case object SHA1 extends DataObfuscation {
    private val md: MessageDigest = MessageDigest.getInstance("SHA-1")

    override def obfuscate(value: String): String =
      Option(value).fold(value) { v =>
        DigestUtils.sha1Hex(md.digest(v.getBytes(StandardCharsets.UTF_8)))
      }
  }

  case class SHA512WithSalt(salt: Array[Byte]) extends DataObfuscation {
    private val md: MessageDigest = MessageDigest.getInstance("SHA-512")
    md.update(salt)

    override def obfuscate(value: String): String = if (value == null) value
    else DigestUtils.sha512Hex(md.digest(value.getBytes(StandardCharsets.UTF_8)))
  }

  case class SHA512WithRandomSalt() extends DataObfuscation {
    private val md: MessageDigest = MessageDigest.getInstance("SHA-512")

    private def generateRandomSalt(): Array[Byte] =
      UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)

    override def obfuscate(value: String): String =
      if (value == null) value
      else {
        md.update(generateRandomSalt())
        DigestUtils.sha512Hex(md.digest(value.getBytes(StandardCharsets.UTF_8)))
      }
  }

}
