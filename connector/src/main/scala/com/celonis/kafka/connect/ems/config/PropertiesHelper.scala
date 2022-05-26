/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.syntax.either._
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

  def error[T](key: String, docs: String): Either[String, T] = s"Invalid [$key]. $docs".asLeft[T]

  private def propertyOr[T](
    props: Map[String, _],
    key:   String,
    docs:  String,
  )(fn:    (Map[String, _], String) => Option[T],
  ): Either[String, T] =
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
