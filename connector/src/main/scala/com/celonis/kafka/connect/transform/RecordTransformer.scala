package com.celonis.kafka.connect.transform

import cats.effect.IO
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.config.ObfuscationConfig
import com.celonis.kafka.connect.ems.conversion.DataConverter
import com.celonis.kafka.connect.ems.errors.FailedObfuscationException
import com.celonis.kafka.connect.ems.storage.PrimaryKeysValidator
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import com.celonis.kafka.connect.transform.flatten.Flattener
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import com.celonis.kafka.connect.ems.obfuscation.ObfuscationUtils._
import cats.syntax.either._
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadata
import com.celonis.kafka.connect.transform.fields.FieldInserter

/**
  * The main business transformation.
  *
  * This class integrates all the components used to transform a connect sink record into an AVRO generic record
  */
final class RecordTransformer(
  sinkName:     String,
  flattener:    Flattener,
  pksValidator: PrimaryKeysValidator,
  obfuscation:  Option[ObfuscationConfig],
  inserter:     FieldInserter,
) extends StrictLogging {
  def transform(sinkRecord: SinkRecord): IO[GenericRecord] = {
    val recordValue = maybeFlattenValue(sinkRecord)

    for {
      transformedValue <- IO(
        inserter.insertFields(
          recordValue,
          EmbeddedKafkaMetadata(sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(), sinkRecord.timestamp()),
        ),
      )
      v <- IO.fromEither(DataConverter.apply(transformedValue))
      _ <- IO(logger.info("[{}] EmsSinkTask:put obfuscation={}", sinkName, obfuscation))
      value <- obfuscation.fold(IO.pure(v)) { o =>
        IO.fromEither(v.obfuscate(o).leftMap(FailedObfuscationException))
      }
      metadata = RecordMetadata(
        TopicPartition(new Topic(sinkRecord.topic()), new Partition(sinkRecord.kafkaPartition())),
        new Offset(sinkRecord.kafkaOffset()),
      )
      _ <- IO.fromEither(pksValidator.validate(value, metadata))
    } yield value
  }

  private def maybeFlattenValue(record: SinkRecord): Any = {
    val value = record.value()
    val valueAndSchema = Option(record.valueSchema()) match {
      case Some(valueSchema) => ValueAndSchema(value, valueSchema)
      case None              => InferSchemaAndNormaliseValue(value).getOrElse(ValueAndSchema(value, Schema.BYTES_SCHEMA))
    }

    flattener.flatten(valueAndSchema.normalisedValue, valueAndSchema.schema)
  }
}

object RecordTransformer {
  def fromConfig(
    sinkName:        String,
    flattenerConfig: Option[FlattenerConfig],
    primaryKeys:     List[String],
    obfuscation:     Option[ObfuscationConfig],
    inserter:      FieldInserter,
  ): RecordTransformer =
    new RecordTransformer(
      sinkName,
      Flattener.fromConfig(flattenerConfig),
      new PrimaryKeysValidator(primaryKeys),
      obfuscation,
      inserter,
    )

  def fromConfig(config: EmsSinkConfig): RecordTransformer =
    fromConfig(
      config.sinkName,
      config.flattenerConfig,
      config.primaryKeys,
      config.obfuscation,
      FieldInserter.embeddedKafkaMetadata(config.embedKafkaMetadata, config.orderField.name)
    )
}
