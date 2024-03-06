/*
 * Copyright 2024 Celonis SE
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

package com.celonis.kafka.connect.transform.fields

trait FieldInserter {
  def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any
}
object FieldInserter {
  val noop = new FieldInserter {
    override def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any = value
  }

  def embeddedKafkaMetadata(doInsert: Boolean, configuredOrderField: Option[String]): FieldInserter =
    (doInsert, configuredOrderField.contains(EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName)) match {
      case (true, true)   => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.All)
      case (true, false)  => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.PartitionOffsetTimestamp)
      case (false, true)  => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.CelonisOrder)
      case (false, false) => noop
    }
}
