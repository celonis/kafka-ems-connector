/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ENDPOINT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_POLICY_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_INTERVAL_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETIRES_DEFAULT
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.NBR_OF_RETRIES_KEY
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
      AUTHORIZATION_KEY,
      Type.PASSWORD,
      null,
      Importance.HIGH,
      AUTHORIZATION_DOC,
    )
    .define(
      ENDPOINT_KEY,
      Type.STRING,
      null,
      Importance.HIGH,
      ENDPOINT_DOC,
    )
    .define(
      TARGET_TABLE_KEY,
      Type.STRING,
      null,
      Importance.HIGH,
      TARGET_TABLE_DOC,
    )
    .define(
      COMMIT_SIZE_KEY,
      Type.LONG,
      null,
      Importance.HIGH,
      COMMIT_SIZE_DOC,
    )
    .define(
      COMMIT_RECORDS_KEY,
      Type.INT,
      null,
      Importance.HIGH,
      COMMIT_RECORDS_DOC,
    )
    .define(
      COMMIT_INTERVAL_KEY,
      Type.LONG,
      null,
      Importance.HIGH,
      COMMIT_INTERVAL_DOC,
    )
    .define(
      TMP_DIRECTORY_KEY,
      Type.STRING,
      null,
      Importance.LOW,
      TMP_DIRECTORY_DOC,
    )
    .define(
      ERROR_POLICY_KEY,
      Type.STRING,
      null,
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

}

class EmsSinkConfigDef() extends ConfigDef with LazyLogging
