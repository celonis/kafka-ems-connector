/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETIRES_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_DOC
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.model.CommitPolicy
import com.celonis.kafka.connect.ems.model.DefaultCommitPolicy
import org.apache.commons.validator.routines.UrlValidator

import java.net.URL
import java.nio.file.Path
import scala.concurrent.duration._

case class EmsSinkConfig(
  sinkName:         String,
  url:              URL,
  target:           String,
  authorizationKey: String,
  errorPolicy:      ErrorPolicy,
  commitPolicy:     CommitPolicy,
  retries:          RetryConfig,
  workingDir:       Path,
)

object EmsSinkConfig {
  private def error[T](key: String, docs: String): Either[String, T] = s"Invalid [$key]. $docs".asLeft[T]

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

  private def nonEmptyStringOr(props: Map[String, _], key: String, docs: String): Either[String, String] =
    propertyOr(props, key, docs)(PropertiesHelper.getString).map(_.trim).flatMap { s =>
      if (s.nonEmpty) s.asRight[String]
      else error(key, docs)
    }

  private def nonEmptyPasswordOr(props: Map[String, _], key: String, docs: String): Either[String, String] =
    propertyOr(props, key, docs)(PropertiesHelper.getPassword).map(_.trim).flatMap { s =>
      if (s.nonEmpty) s.asRight[String]
      else error(key, docs)
    }

  private def longOr(props: Map[String, _], key: String, docs: String): Either[String, Long] =
    propertyOr(props, key, docs)(PropertiesHelper.getLong)

  def extractURL(props: Map[String, _]): Either[String, URL] =
    nonEmptyStringOr(props, ENDPOINT_KEY, ENDPOINT_DOC).flatMap { value =>
      if (!new UrlValidator(Array("https")).isValid(value)) error(ENDPOINT_KEY, ENDPOINT_DOC)
      else new URL(value).asRight[String]
    }

  def extractTargetTable(props: Map[String, _]): Either[String, String] =
    nonEmptyStringOr(props, TARGET_TABLE_KEY, TARGET_TABLE_DOC)

  def extractAuthorizationHeader(props: Map[String, _]): Either[String, String] =
    nonEmptyPasswordOr(props, AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      .map(_.trim)
      .flatMap { auth =>
        if (auth.startsWith("AppKey") || auth.startsWith("Bearer")) auth.asRight
        else error(AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      }

  def extractErrorPolicy(props: Map[String, _]): Either[String, ErrorPolicy] =
    nonEmptyStringOr(props, ERROR_POLICY_KEY, ERROR_POLICY_DOC)
      .flatMap { constant =>
        ErrorPolicy.withNameInsensitiveOption(constant) match {
          case Some(value) => value.asRight[String]
          case None        => error(ERROR_POLICY_KEY, ERROR_POLICY_DOC)
        }
      }

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

  def extractCommitPolicy(props: Map[String, _]): Either[String, CommitPolicy] =
    for {
      size <- longOr(props, COMMIT_SIZE_KEY, COMMIT_SIZE_DOC).flatMap { l =>
        if (l < 1000000L) error(COMMIT_SIZE_KEY, "Flush size needs to be at least 1000000 (1 MB).")
        else l.asRight[String]
      }
      records <- longOr(props, COMMIT_RECORDS_KEY, COMMIT_RECORDS_DOC).flatMap { l =>
        if (l <= 0) error(COMMIT_RECORDS_KEY, "Uploading the data to EMS requires a record count greater than 0.")
        else l.asRight[String]
      }
      interval <- longOr(props, COMMIT_INTERVAL_KEY, COMMIT_INTERVAL_DOC).flatMap { l =>
        if (l <= 1000)
          error(COMMIT_INTERVAL_KEY, "The stop gap interval for uploading the data cannot be smaller than 1000 (1s).")
        else l.asRight[String]
      }
    } yield DefaultCommitPolicy(size, interval.millis, records)

  def extractWorkingDirectory(props: Map[String, _]): Either[String, Path] =
    nonEmptyStringOr(props, TMP_DIRECTORY_KEY, TMP_DIRECTORY_DOC)
      .flatMap { value =>
        val path = Path.of(value)
        val file = path.toFile
        if (!file.exists()) {
          if (!file.mkdir()) {
            error(TMP_DIRECTORY_KEY, s"Cannot create the folder:[$path].")
          } else path.asRight[String]
        } else {
          if (!file.isDirectory)
            error(TMP_DIRECTORY_KEY, s"Folder [$path] is pointing to a file.")
          else path.asRight[String]
        }
      }

  def from(sinkName: String, props: Map[String, _]): Either[String, EmsSinkConfig] =
    for {
      url           <- extractURL(props)
      table         <- extractTargetTable(props)
      authorization <- extractAuthorizationHeader(props)
      error         <- extractErrorPolicy(props)
      commitPolicy  <- extractCommitPolicy(props)
      retry         <- extractRetry(props)
      tempDir       <- extractWorkingDirectory(props)
    } yield EmsSinkConfig(
      sinkName,
      url,
      table,
      authorization,
      error,
      commitPolicy,
      retry,
      tempDir,
    )
}
