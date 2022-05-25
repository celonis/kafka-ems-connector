/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.syntax.either._

case class RetryConfig(retries: Int, interval: Long)

object RetryConfig {
  import EmsSinkConfigConstants._
  import PropertiesHelper._

  def extractRetry(props: Map[String, _]): Either[String, RetryConfig] =
    for {
      interval <- PropertiesHelper.getLong(props, ERROR_RETRY_INTERVAL) match {
        case Some(value) =>
          if (value < 1000) error(ERROR_RETRY_INTERVAL, "Retry interval cannot be smaller than 1000 (1s).")
          else value.asRight[String]
        case None => ERROR_RETRY_INTERVAL_DEFAULT.asRight[String]
      }
      retries <- PropertiesHelper.getInt(props, NBR_OF_RETRIES_KEY) match {
        case Some(value) =>
          if (value <= 0) error(NBR_OF_RETRIES_KEY, "Number of retries needs to be greater than 0.")
          else value.asRight[String]
        case None => NBR_OF_RETIRES_DEFAULT.asRight[String]
      }
    } yield RetryConfig(retries, interval)

}
