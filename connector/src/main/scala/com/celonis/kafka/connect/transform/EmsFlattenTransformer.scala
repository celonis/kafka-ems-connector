/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.transform.flatten.ChunkedJsonBlob
import com.celonis.kafka.connect.transform.flatten.Flattener
import com.celonis.kafka.connect.transform.flatten.SchemaFlattener
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.transforms.Transformation

import java.util

class EmsFlattenTransformer[R <: ConnectRecord[R]] extends Transformation[R] with LazyLogging {

  private implicit var transformerConfig: FlattenerConfig = FlattenerConfig()

  override def config(): ConfigDef =
    FlattenerConfig.configDef

  override def configure(configs: util.Map[String, _]): Unit =
    transformerConfig = FlattenerConfig(configs)

  private implicit class RecordExt(record: R) {
    def newRecordWith(value: Any, schema: Schema): R =
      record.newRecord(
        record.topic,
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        schema,
        value,
        record.timestamp(),
      )
  }

  override def apply(record: R): R = {
    val value = record.value()
    val maybeSchema =
      Option(record.valueSchema()).map(_ -> false)
        .orElse(SchemaInference(record.value()).map(_ -> true))

    maybeSchema.map {
      case (schema, schemaIsInferred) =>
        val newValueSchema = SchemaFlattener.flatten(schema)
        val newValue       = Flattener.flatten(value, newValueSchema, schemaIsInferred)

        record.newRecordWith(
          newValue,
          newValueSchema,
        )

    }.getOrElse {
      transformerConfig.jsonBlobChunks.fold(record) { implicit jsonBlobConfig =>
        val newValue = ChunkedJsonBlob.asConnectData(value)
        record.newRecordWith(
          newValue,
          newValue.schema(),
        )
      }
    }
  }

  override def close(): Unit = {}

}
