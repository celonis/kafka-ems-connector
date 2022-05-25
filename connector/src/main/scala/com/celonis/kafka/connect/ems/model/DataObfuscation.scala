/*
 * Copyright 2017-2022 Celonis Ltd
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
