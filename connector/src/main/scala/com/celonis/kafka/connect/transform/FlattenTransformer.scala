/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.transform.flatten.Flattener
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation

import java.util

class FlattenTransformer[R <: ConnectRecord[R]] extends Transformation[R] with LazyLogging {

  private implicit var transformerConfig: FlattenConfig = FlattenConfig()

  override def config(): ConfigDef =
    FlattenConfig.configDef

  override def configure(configs: util.Map[String, _]): Unit =
    transformerConfig = FlattenConfig(configs)

  override def apply(record: R): R = {
    val value       = record.value()
    val maybeSchema = Option(record.valueSchema())

    maybeSchema.map { schema =>
      val newValueSchema = SchemaFlattener.flatten(schema)
      val newValue       = Flattener.flatten(value, newValueSchema)
      record.newRecord(
        record.topic,
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newValueSchema,
        newValue,
        record.timestamp(),
      )

    }.getOrElse(record)
  }

  override def close(): Unit = {}

}
