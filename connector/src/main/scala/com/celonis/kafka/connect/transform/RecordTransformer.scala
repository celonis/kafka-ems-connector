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

package com.celonis.kafka.connect.transform

import cats.effect.IO
import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.config.ObfuscationConfig
import com.celonis.kafka.connect.ems.conversion.DataConverter
import com.celonis.kafka.connect.ems.errors.FailedObfuscationException
import com.celonis.kafka.connect.ems.model._
import com.celonis.kafka.connect.ems.obfuscation.ObfuscationUtils._
import com.celonis.kafka.connect.ems.storage.PrimaryKeysValidator
import com.celonis.kafka.connect.transform.conversion.ConnectConversion
import com.celonis.kafka.connect.transform.fields.EmbeddedKafkaMetadata
import com.celonis.kafka.connect.transform.fields.FieldInserter
import com.celonis.kafka.connect.transform.flatten.Flattener
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.sink.SinkRecord

/** The main business transformation.
  *
  * This class integrates all the components used to transform a connect sink record into an AVRO generic record
  */
final class RecordTransformer(
  sinkName:      String,
  preConversion: ConnectConversion,
  flattener:     Flattener,
  pksValidator:  PrimaryKeysValidator,
  obfuscation:   Option[ObfuscationConfig],
  inserter:      FieldInserter,
) extends StrictLogging {
  def transform(sinkRecord: SinkRecord): IO[GenericRecord] = {
    val (convertedValue, convertedSchema) = preConversion.convert(sinkRecord.value(), Option(sinkRecord.valueSchema()))
    val flattenedValue                    = flattener.flatten(convertedValue, convertedSchema)

    for {
      transformedValue <- IO(
        inserter.insertFields(
          flattenedValue,
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
}

object RecordTransformer {
  def fromConfig(
    sinkName:            String,
    preConversionConfig: PreConversionConfig,
    flattenerConfig:     Option[FlattenerConfig],
    primaryKeys:         List[String],
    obfuscation:         Option[ObfuscationConfig],
    allowNullsAsPks:     Boolean,
    inserter:            FieldInserter): RecordTransformer =
    new RecordTransformer(
      sinkName,
      ConnectConversion.fromConfig(preConversionConfig),
      Flattener.fromConfig(flattenerConfig),
      new PrimaryKeysValidator(primaryKeys, allowNullsAsPks),
      obfuscation,
      inserter,
    )

  def fromConfig(config: EmsSinkConfig): RecordTransformer =
    fromConfig(
      config.sinkName,
      config.preConversionConfig,
      config.flattenerConfig,
      config.primaryKeys,
      config.obfuscation,
      config.allowNullsAsPks,
      FieldInserter.embeddedKafkaMetadata(config.embedKafkaMetadata, config.orderField.name),
    )
}
