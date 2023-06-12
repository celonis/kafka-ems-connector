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
import org.apache.kafka.common.config.types.Password

import scala.util.Try

object PropertiesHelper {
  def getString(props: Map[String, _], key: String): Option[String] =
    props.get(key).collect {
      case s: String if s.nonEmpty => s
      case p: Password             => p.value()
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

  def error[T](key: String, docs: String): Either[String, T] = s"Invalid [$key]. $docs".asLeft[T]

  private def propertyOr[T](
    props: Map[String, _],
    key:   String,
    docs:  String,
  )(fn: (Map[String, _], String) => Option[T]): Either[String, T] =
    fn(props, key) match {
      case Some(value) => value.asRight[String]
      case None        => error(key, docs)
    }

  def nonEmptyStringOr(props: Map[String, _], key: String, docs: String): Either[String, String] =
    propertyOr(props, key, docs)(PropertiesHelper.getString).map(_.trim).flatMap { s =>
      if (s.nonEmpty) s.asRight[String]
      else error(key, docs)
    }

  def nonEmptyPasswordOr(props: Map[String, _], key: String, docs: String): Either[String, String] =
    propertyOr(props, key, docs)(PropertiesHelper.getPassword).map(_.trim).flatMap { s =>
      if (s.nonEmpty) s.asRight[String]
      else error(key, docs)
    }

  def longOr(props: Map[String, _], key: String, docs: String): Either[String, Long] =
    propertyOr(props, key, docs)(PropertiesHelper.getLong)

  def booleanOr(props: Map[String, _], key: String, docs: String): Either[String, Boolean] =
    propertyOr(props, key, docs)(PropertiesHelper.getBoolean)

}
