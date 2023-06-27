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

  val PARQUET_ROW_GROUP_SIZE_BYTES_KEY:     String = s"$CONNECTOR_PREFIX.parquet.write.flush.records"
  val PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT: Int    = 1024 * 1024
  val PARQUET_ROW_GROUP_SIZE_BYTES_DOC: String =
    s"""
       |The number of bytes of the row groups in the Parquet file.
       | Default is $PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT.
       | """.stripMargin

  val NBR_OF_RETRIES_KEY = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC =
    "The maximum number of times to re-attempt to write the records before the task is marked as failed."
  val NBR_OF_RETIRES_DEFAULT: Int = 10

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

  val ORDER_FIELD_NAME_KEY = s"$CONNECTOR_PREFIX.order.field.name"
  val ORDER_FIELD_NAME_DOC =
    s"The name of a sortable field present in the data data. If it's not provided the connector injects its own."

  val CONNECTION_POOL_MAX_IDLE_CONNECTIONS_KEY           = s"$CONNECTOR_PREFIX.pool.max.idle"
  val CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DOC           = "Connection pool - Max idle connections"
  val CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE = 5

  val CONNECTION_POOL_KEEPALIVE_MILLIS_KEY           = s"$CONNECTOR_PREFIX.pool.keepalive"
  val CONNECTION_POOL_KEEPALIVE_MILLIS_DOC           = "Connection pool - Keep Alive Millis"
  val CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE = 300000L

  val CLOSE_EVERY_CONNECTION_KEY           = s"$CONNECTOR_PREFIX.pool.explicit.close"
  val CLOSE_EVERY_CONNECTION_DOC           = "Connection pool - Explicitly close connections"
  val CLOSE_EVERY_CONNECTION_DEFAULT_VALUE = false

  val FLATTENER_ENABLE_KEY = s"${CONNECTOR_PREFIX}.flattener.enable"
  val FLATTENER_ENABLE_DOC =
    s"Enable flattening of nested records. This is likely to be needed if the source data contains nested objects or collections."
  val FLATTENER_ENABLE_DEFAULT = false

  val FLATTENER_DISCARD_COLLECTIONS_KEY = s"${CONNECTOR_PREFIX}.flattener.collections.discard"
  val FLATTENER_DISCARD_COLLECTIONS_DOC =
    "Discard array and map fields at any level of depth. Note that the default handling of collections by the flattener function is to JSON-encode them as nullable STRING fields."
  val FLATTENER_DISCARD_COLLECTIONS_DEFAULT = false

  val FLATTENER_JSONBLOB_CHUNKS_KEY = s"${CONNECTOR_PREFIX}.flattener.jsonblob.chunks"
  val FLATTENER_JSONBLOB_CHUNKS_DOC =
    "Encodes the record into a JSON blob broken down into N VARCHAR fields (e.g. `payload_chunk1`, `payload_chunk2`, `...`, `payload_chunkN`)."
  val FLATTENER_JSONBLOB_CHUNKS_DEFAULT = null

  val DECIMAL_CONVERSION_KEY = s"${CONNECTOR_PREFIX}.convert.decimals.to.double"
  val DECIMAL_CONVERSION_KEY_DOC =
    s"Convert decimal values into doubles. Valid only for formats with schema (AVRO, Protobuf, JsonSchema)"
  val DECIMAL_CONVERSION_KEY_DEFAULT = false

  val NULL_PK_KEY = s"${CONNECTOR_PREFIX}.allow.null.pk"
  val NULL_PK_KEY_DOC =
    s"Allow parsing messages with null values in the columns listed as primary keys. If disabled connector will fail after receiving such a message. NOTE: enabling that will cause data inconsistency issue on the EMS side."
  val NULL_PK_KEY_DEFAULT = false

  val EMBED_KAFKA_EMBEDDED_METADATA_KEY = s"${CONNECTOR_PREFIX}.embed.kafka.metadata"
  val EMBED_KAFKA_EMBEDDED_METADATA_DOC =
    "Embed Kafka metadata such as partition, offset and timestamp as additional record fields."
  val EMBED_KAFKA_EMBEDDED_METADATA_DEFAULT = true

  val USE_IN_MEMORY_FS_KEY = s"${CONNECTOR_PREFIX}.inmemfs.enable"
  val USE_IN_MEMORY_FS_DOC =
    "Rather than writing to the host file system, buffer parquet data files in memory"
  val USE_IN_MEMORY_FS_DEFAULT = false

}
