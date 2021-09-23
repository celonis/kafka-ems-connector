/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config
import org.apache.kafka.common.config.types.Password

import scala.util.Try

object PropertiesHelper {
  def getString(props: Map[String, _], key: String): Option[String] =
    props.get(key).collect {
      case s: String   => s
      case p: Password => p.value()
    }.flatMap(Option(_))

  def getLong(props: Map[String, _], key: String): Option[Long] =
    props.get(key).flatMap(Option(_)).collect {
      case long: Long => long
      case i:    Int  => i.toLong
    }

  def getInt(props: Map[String, _], key: String): Option[Int] =
    props.get(key).flatMap(Option(_)).collect { case i: Int => i }

  def getPassword(props: Map[String, _], key: String): Option[String] =
    props.get(key).flatMap(Option(_)).collect {
      case p: Password => p.value()
      case s: String   => s
    }

  def getBoolean(props: Map[String, _], key: String): Option[Boolean] =
    props.get(key).flatMap(Option(_)).collect {
      case b: Boolean                              => b
      case s: String if Try(s.toBoolean).isSuccess => s.toBoolean
    }

}
