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

import org.apache.kafka.common.config.ConfigDef
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

// TODO: dedupe with EmsSinkConfigDef
object EmsStreamingSinkConfigDef {
  val config: ConfigDef = new ConfigDef()
    .define(
      ENDPOINT_KEY,
      Type.STRING,
      Importance.HIGH,
      ENDPOINT_DOC_STREAMING,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      "Endpoint",
    )
    .define(
      DATA_POOL_ID_KEY,
      Type.STRING,
      Importance.HIGH,
      DATA_POOL_ID_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      "Data Pool ID",
    )
    .define(
      AUTHORIZATION_KEY,
      Type.PASSWORD,
      Importance.HIGH,
      AUTHORIZATION_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      "Authorization Key",
    )
    .define(
      TARGET_TABLE_KEY,
      Type.STRING,
      Importance.HIGH,
      TARGET_TABLE_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      "Table",
    )
    .define(
      KEY_SCHEMA_KEY,
      Type.STRING,
      KEY_SCHEMA_DEFAULT,
      Importance.HIGH,
      KEY_SCHEMA_DOC,
      "Data",
      1,
      ConfigDef.Width.MEDIUM,
      "Key Schema",
    )
    .define(
      VALUE_SCHEMA_KEY,
      Type.STRING,
      Importance.HIGH,
      VALUE_SCHEMA_DOC,
      "Data",
      2,
      ConfigDef.Width.MEDIUM,
      "Value Schema",
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

  val taskConfig = new ConfigDef(config)
    .define(
      TARGET_TOPIC_KEY,
      Type.STRING,
      Importance.HIGH,
      TARGET_TOPIC_DOC,
      "Data",
      3,
      ConfigDef.Width.MEDIUM,
      "Target Topic",
    )
}
