/*
 * Copyright 2022 Celonis SE
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
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_ID_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_ID_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_ID_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DEBUG_KEEP_TMP_FILES_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DEBUG_KEEP_TMP_FILES_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.DEBUG_KEEP_TMP_FILES_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.EXPLODE_MODE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.EXPLODE_MODE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETIRES_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATED_FIELDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATED_FIELDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATION_TYPE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATION_TYPE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PARQUET_FLUSH_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PRIMARY_KEYS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHBASIC_PASSWORD_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHBASIC_PASSWORD_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHBASIC_USERNAME_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHBASIC_USERNAME_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHENTICATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_AUTHENTICATION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_HOST_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_HOST_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_PORT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_PORT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_TYPE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.PROXY_TYPE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_RANDOM_SALT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_RANDOM_SALT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_SALT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_SALT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TARGET_TABLE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.TMP_DIRECTORY_KEY
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object EmsSinkConfigDef {
  val config: ConfigDef = new EmsSinkConfigDef()
    .define(
      ENDPOINT_KEY,
      Type.STRING,
      Importance.HIGH,
      ENDPOINT_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      "Endpoint",
    )
    .define(
      AUTHORIZATION_KEY,
      Type.PASSWORD,
      Importance.HIGH,
      AUTHORIZATION_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      "Authorization Key",
    )
    .define(
      TARGET_TABLE_KEY,
      Type.STRING,
      Importance.HIGH,
      TARGET_TABLE_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      "Table",
    )
    .define(
      CONNECTION_ID_KEY,
      Type.STRING,
      CONNECTION_ID_DEFAULT,
      Importance.HIGH,
      CONNECTION_ID_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      "Connection Id",
    )
    .define(
      COMMIT_SIZE_KEY,
      Type.LONG,
      Importance.HIGH,
      COMMIT_SIZE_DOC,
      "Commit",
      1,
      ConfigDef.Width.MEDIUM,
      "File Size",
    )
    .define(
      COMMIT_RECORDS_KEY,
      Type.INT,
      Importance.HIGH,
      COMMIT_RECORDS_DOC,
      "Commit",
      2,
      ConfigDef.Width.MEDIUM,
      "Max records",
    )
    .define(
      COMMIT_INTERVAL_KEY,
      Type.LONG,
      Importance.HIGH,
      COMMIT_INTERVAL_DOC,
      "Commit",
      3,
      ConfigDef.Width.MEDIUM,
      "Time window",
    )
    .define(
      TMP_DIRECTORY_KEY,
      Type.STRING,
      Importance.LOW,
      TMP_DIRECTORY_DOC,
    )
    .define(
      PRIMARY_KEYS_KEY,
      Type.STRING,
      PRIMARY_KEYS_DEFAULT,
      Importance.LOW,
      PRIMARY_KEYS_DOC,
      "Data",
      1,
      ConfigDef.Width.MEDIUM,
      "Primary Keys",
    )
    .define(
      FALLBACK_VARCHAR_LENGTH_KEY,
      Type.INT,
      FALLBACK_VARCHAR_LENGTH_DEFAULT,
      Importance.LOW,
      FALLBACK_VARCHAR_LENGTH_DOC,
      "Data",
      2,
      ConfigDef.Width.MEDIUM,
      "String(VARCHAR) type size",
    )
    .define(
      ERROR_POLICY_KEY,
      Type.STRING,
      ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      ERROR_POLICY_DOC,
    )
    .define(
      NBR_OF_RETRIES_KEY,
      Type.INT,
      NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      NBR_OF_RETRIES_KEY,
    )
    .define(
      ERROR_RETRY_INTERVAL,
      Type.LONG,
      ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      ERROR_RETRY_INTERVAL,
    )
    .define(
      PARQUET_FLUSH_KEY,
      Type.INT,
      PARQUET_FLUSH_DEFAULT,
      Importance.MEDIUM,
      PARQUET_FLUSH_DOC,
      "Parquet",
      1,
      ConfigDef.Width.LONG,
      PARQUET_FLUSH_KEY,
    )
    .define(
      DEBUG_KEEP_TMP_FILES_KEY,
      Type.BOOLEAN,
      DEBUG_KEEP_TMP_FILES_DEFAULT,
      Importance.LOW,
      DEBUG_KEEP_TMP_FILES_DOC,
      "Parquet",
      2,
      ConfigDef.Width.SHORT,
      DEBUG_KEEP_TMP_FILES_KEY,
    )
    .define(
      OBFUSCATED_FIELDS_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      OBFUSCATED_FIELDS_DOC,
      "Obfuscation",
      1,
      ConfigDef.Width.LONG,
      OBFUSCATED_FIELDS_DOC,
    )
    .define(
      OBFUSCATION_TYPE_KEY,
      Type.STRING,
      "FIX",
      Importance.LOW,
      OBFUSCATION_TYPE_DOC,
      "Obfuscation",
      2,
      ConfigDef.Width.SHORT,
      OBFUSCATION_TYPE_KEY,
    )
    .define(
      SHA512_SALT_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      SHA512_SALT_DOC,
      "Obfuscation",
      3,
      ConfigDef.Width.MEDIUM,
      SHA512_SALT_DOC,
    )
    .define(
      SHA512_RANDOM_SALT_KEY,
      Type.BOOLEAN,
      null,
      Importance.LOW,
      SHA512_RANDOM_SALT_DOC,
      "Obfuscation",
      4,
      ConfigDef.Width.MEDIUM,
      SHA512_RANDOM_SALT_DOC,
    )
    .define(
      PROXY_HOST_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      PROXY_HOST_DOC,
      "Proxy",
      1,
      ConfigDef.Width.MEDIUM,
      PROXY_HOST_DOC,
    )
    .define(
      PROXY_PORT_KEY,
      Type.INT,
      null,
      Importance.LOW,
      PROXY_PORT_DOC,
      "Proxy",
      2,
      ConfigDef.Width.MEDIUM,
      PROXY_PORT_DOC,
    )
    .define(
      PROXY_TYPE_KEY,
      Type.STRING,
      "HTTP",
      Importance.LOW,
      PROXY_TYPE_DOC,
      "Proxy",
      3,
      ConfigDef.Width.MEDIUM,
      PROXY_TYPE_DOC,
    )
    .define(
      PROXY_AUTHENTICATION_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      PROXY_AUTHENTICATION_DOC,
      "Proxy",
      4,
      ConfigDef.Width.MEDIUM,
      PROXY_AUTHENTICATION_DOC,
    )
    .define(
      PROXY_AUTHBASIC_USERNAME_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      PROXY_AUTHBASIC_USERNAME_DOC,
      "Proxy",
      5,
      ConfigDef.Width.MEDIUM,
      PROXY_AUTHBASIC_USERNAME_DOC,
    )
    .define(
      PROXY_AUTHBASIC_PASSWORD_KEY,
      Type.PASSWORD,
      null,
      Importance.LOW,
      PROXY_AUTHBASIC_PASSWORD_DOC,
      "Proxy",
      6,
      ConfigDef.Width.MEDIUM,
      PROXY_AUTHBASIC_PASSWORD_DOC,
    )
    .define(
      EXPLODE_MODE_KEY,
      Type.STRING,
      "None",
      Importance.LOW,
      EXPLODE_MODE_DOC,
      "Explode",
      1,
      ConfigDef.Width.MEDIUM,
      EXPLODE_MODE_DOC,
    )
    .define(
      ORDER_FIELD_NAME_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      EXPLODE_MODE_DOC,
      "Data",
      3,
      ConfigDef.Width.MEDIUM,
      ORDER_FIELD_NAME_DOC,
    )
}

class EmsSinkConfigDef() extends ConfigDef with LazyLogging
