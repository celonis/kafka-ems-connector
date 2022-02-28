/*
 * Copyright 2017-2022 Celonis Ltd
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

  val PRIMARY_KEYS_KEY: String = s"$CONNECTOR_PREFIX.data.primary.key"
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
       | """.stripMargin

  val NBR_OF_RETRIES_KEY = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC =
    "The maximum number of times to re-attempt to write the records before the task is marked as failed."
  val NBR_OF_RETIRES_DEFAULT: Int = 10

  val CLIENT_ID_KEY = s"$CONNECTOR_PREFIX.client.id"
  val CLIENT_ID_DOC =
    "Optional parameter representing the client unique identifier"
  val CLIENT_ID_DEFAULT: String = null

  val ERROR_POLICY_KEY = s"$CONNECTOR_PREFIX.error.policy"
  val ERROR_POLICY_DOC: String =
    s"""
       |Specifies the action to be taken if an error occurs while inserting the data.
       | There are three available options:
       |    CONTINUE - the error is swallowed
       |    THROW - the error is allowed to propagate.
       |    RETRY - The exception causes the Connect framework to retry the message. The number of retries is set by $NBR_OF_RETRIES_KEY.
       |All errors will be logged automatically, even if the code swallows them.
    """.stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL     = s"$CONNECTOR_PREFIX.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT: Long = 60000L

  val FALLBACK_VARCHAR_LENGTH_KEY = s"$CONNECTOR_PREFIX.data.fallback.varchar.length"
  val FALLBACK_VARCHAR_LENGTH_DOC =
    "Optional parameter representing the STRING (VARCHAR) length when the schema is created in EMS"
  val FALLBACK_VARCHAR_LENGTH_DEFAULT: Integer = null

  val DEBUG_KEEP_TMP_FILES_KEY: String = s"$CONNECTOR_PREFIX.debug.keep.parquet.files"
  val DEBUG_KEEP_TMP_FILES_DOC: String =
    s"For debug purpose, set the setting to true for the connector to keep the local files after an upload. Default is false."
  val DEBUG_KEEP_TMP_FILES_DEFAULT = false

  val OBFUSCATED_FIELDS_KEY = s"$CONNECTOR_PREFIX.obfuscation.fields"
  val OBFUSCATED_FIELDS_DOC =
    s"Comma separated list of fields to be obfuscated. Example: a, a.b, a.array_field.nested_field, a.array_field."

  val SHA512_SALT_KEY = s"$CONNECTOR_PREFIX.obfuscation.sha512.salt"
  val SHA512_SALT_DOC = s"The required salt value when using SHA512 obfuscation."

  val SHA512_RANDOM_SALT_KEY = s"$CONNECTOR_PREFIX.obfuscation.sha512.randomsalt"
  val SHA512_RANDOM_SALT_DOC = s"Salt is randomly generated for each call to obfuscate a field."

  val OBFUSCATION_TYPE_KEY = s"$CONNECTOR_PREFIX.obfuscation.method"
  val OBFUSCATION_TYPE_DOC =
    s"The obfuscation type. Only required if obfuscation fields are provided. Available values are: FIX,SHA1 and SHA512. For SHA512 a salt is required via $SHA512_SALT_KEY configuration, or a random salt can be configured using $SHA512_RANDOM_SALT_KEY."

  val PROXY_HOST_KEY = s"$CONNECTOR_PREFIX.proxy.host"
  val PROXY_HOST_DOC =
    s"Proxy host excluding port, eg my-proxy.com"

  val PROXY_PORT_KEY = s"$CONNECTOR_PREFIX.proxy.port"
  val PROXY_PORT_DOC =
    s"The port number for the proxy server"

  val PROXY_AUTHENTICATION_KEY = s"$CONNECTOR_PREFIX.proxy.auth.type"
  val PROXY_AUTHENTICATION_DOC =
    s"Proxy authentication type.  Only one option at present, BASIC"

  val PROXY_AUTHBASIC_USERNAME_KEY = s"$CONNECTOR_PREFIX.proxy.auth.username"
  val PROXY_AUTHBASIC_USERNAME_DOC =
    s"Proxy BASIC auth username"

  val PROXY_AUTHBASIC_PASSWORD_KEY = s"$CONNECTOR_PREFIX.proxy.auth.password"
  val PROXY_AUTHBASIC_PASSWORD_DOC =
    s"Proxy BASIC auth password"

  val PROXY_TYPE_KEY = s"$CONNECTOR_PREFIX.proxy.type"
  val PROXY_TYPE_DOC =
    s"Proxy type, can be HTTP, SOCKS4 or SOCKS5"

  val EXPLODE_MODE_KEY = s"$CONNECTOR_PREFIX.explode.mode"
  val EXPLODE_MODE_DOC = s"Explode (flatten out) a top-level collection, can be set to None or List"

}