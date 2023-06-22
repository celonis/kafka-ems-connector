package com.celonis.kafka.connect.ems.config
import PropertiesHelper._
import EmsSinkConfigConstants._
import cats.implicits.catsSyntaxEitherId

final case class CommitPolicyConfig(fileSize: Long, interval: Long, records: Long)

object CommitPolicyConfig {
  def extract(props: Map[String, _]): Either[String, CommitPolicyConfig] =
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
    } yield CommitPolicyConfig(size, interval, records)

}
