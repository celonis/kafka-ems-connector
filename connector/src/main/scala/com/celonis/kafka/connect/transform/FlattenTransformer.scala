/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform

import cats.implicits._
import com.celonis.kafka.connect.transform.flatten.Flattener
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.errors.ConnectException
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

    {
      for {
        newValueSchema <- SchemaFlattener.flatten(maybeSchema)
        newValue        = Flattener.flatten(value, newValueSchema)
        newRecord = record.newRecord(
          record.topic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          newValueSchema.orNull,
          newValue.getOrElse(null),
          record.timestamp(),
        )
      } yield newRecord
    }.leftMap {
      e =>
        logger.error(s"Error flattening $e")
        throw new ConnectException(e)
    }.merge
  }

  override def close(): Unit = {}

}
