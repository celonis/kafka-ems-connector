/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

object EmsSinkConfigConstants {
  val CONNECTOR_PREFIX = "connect.ems"

  val AUTHORIZATION_KEY: String = s"$CONNECTOR_PREFIX.authorization.key"
  val AUTHORIZATION_DOC =
    "Contains the EMS API Authorization header. It should be [AppKey <<app-key>>] or [Bearer <<api-key>>]."

  val TARGET_TABLE_KEY: String = s"$CONNECTOR_PREFIX.target.table"
  val TARGET_TABLE_DOC: String = s"The table in EMS to store the data."

  val CONNECTION_ID_KEY: String = s"$CONNECTOR_PREFIX.connection.id"
  val CONNECTION_ID_DOC: String =
    s"Optional parameter. It represents the unique EMS connection identifier."
  val CONNECTION_ID_DEFAULT: String = null

  val ENDPOINT_KEY: String = s"$CONNECTOR_PREFIX.endpoint"
  val ENDPOINT_DOC: String =
    s"Contains the EMS API endpoint in the form of:https://<<team>>.<<realm>>.celonis.cloud/continuous-batch-processing/api/v1/<<pool-id>>/items."

  val TMP_DIRECTORY_KEY: String = s"$CONNECTOR_PREFIX.tmp.dir"
  val TMP_DIRECTORY_DOC: String =
    s"The folder to store the temporary files as it accumulates data. If not specified then [${System.getProperty("java.io.tmpdir")}] is being used."

  val PRIMARY_KEYS_KEY: String = s"$CONNECTOR_PREFIX.primary.key"
  val PRIMARY_KEYS_DOC: String =
    "Optional field containing comma separated fields values which should be made primary key for the table constructed in EMS."
  val PRIMARY_KEYS_DEFAULT: String = null

  val COMMIT_SIZE_KEY: String = s"$CONNECTOR_PREFIX.commit.size.bytes"
  val COMMIT_SIZE_DOC: String =
    "The accumulated file maximum size before it is uploaded to EMS. It cannot be less than 1MB."

  val COMMIT_RECORDS_KEY: String = s"$CONNECTOR_PREFIX.commit.records"
  val COMMIT_RECORDS_DOC: String =
    s"The maximum number of records in the accumulated file before it is uploaded to EMS."

  val COMMIT_INTERVAL_KEY: String = s"$CONNECTOR_PREFIX.commit.interval.ms"
  val COMMIT_INTERVAL_DOC: String =
    s"The time interval in milliseconds to upload the data to EMS if the other two commit policies are not yet applicable."

  val PARQUET_FLUSH_KEY:     String = s"$CONNECTOR_PREFIX.parquet.write.flush.records"
  val PARQUET_FLUSH_DEFAULT: Int    = 1000
  val PARQUET_FLUSH_DOC: String =
    s"""
       |The number of records after which it should flush the parquet file, to ensure the file size policy.
       | Default is $PARQUET_FLUSH_DEFAULT.
       | Writing parquet allows to flush the buffer only when the file is closed. This results in a file closed and a file open sequence.
       | This is not related to or impacts $COMMIT_RECORDS_KEY.""".stripMargin

  val PROGRESS_COUNTER_ENABLED: String = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed."
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val ERROR_POLICY_KEY = s"$CONNECTOR_PREFIX.error.policy"
  val ERROR_POLICY_DOC: String =
    """
      |Specifies the action to be taken if an error occurs while inserting the data.
      | There are three available options:
      |    CONTINUE - the error is swallowed
      |    THROW - the error is allowed to propagate.
      |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by connect.s3.max.retries.
      |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL     = s"$CONNECTOR_PREFIX.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT: Long = 60000L

  val NBR_OF_RETRIES_KEY = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT: Int = 20

  val DEBUG_KEEP_TMP_FILES_KEY: String = s"$CONNECTOR_PREFIX.debug.keep.parquet.files"
  val DEBUG_KEEP_TMP_FILES_DOC: String =
    s"For debug purpose, set the setting to true for the connector to keep the local files after an upload. Default is false."
  val DEBUG_KEEP_TMP_FILES_DEFAULT = false
}
