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

package com.celonis.kafka.connect.ems.storage
import cats.implicits.toShow
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import com.celonis.kafka.connect.ems.model._
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters._

class PrimaryKeysValidatorTests extends AnyFunSuite with Matchers with MockitoSugar {
  test("empty pks return success") {
    val record = mock[GenericRecord]
    when(record.getSchema).thenReturn(Schema.createRecord(
      "r",
      "",
      "ns",
      false,
      List(new Schema.Field("a", SchemaBuilder.builder.stringType())).asJava,
    ))

    val validator = new PrimaryKeysValidator(Nil)
    val metadata  = RecordMetadata(TopicPartition(new Topic("a"), new Partition(1)), new Offset(100))
    validator.validate(record, metadata) shouldBe Right(())
  }

  test("returns an error if the field is not present") {
    val record = mock[GenericRecord]
    when(record.getSchema).thenReturn(
      Schema.createRecord(
        "r",
        "",
        "ns",
        false,
        List(
          new Schema.Field("a", SchemaBuilder.builder.stringType()),
          new Schema.Field("b", SchemaBuilder.builder.booleanType()),
          new Schema.Field("c", SchemaBuilder.builder.intType()),
        ).asJava,
      ),
    )

    val validator = new PrimaryKeysValidator(List("d"))
    val metadata  = RecordMetadata(TopicPartition(new Topic("a"), new Partition(1)), new Offset(100))
    validator.validate(record, metadata) shouldBe Left(
      InvalidInputException(s"Incoming record is missing these primary key(-s):d for record ${metadata.show}"),
    )
  }

  test("returns an error if one of the fields is not present") {
    val record = mock[GenericRecord]
    when(record.getSchema).thenReturn(
      Schema.createRecord(
        "r",
        "",
        "ns",
        false,
        List(
          new Schema.Field("a", SchemaBuilder.builder.stringType()),
          new Schema.Field("b", SchemaBuilder.builder.booleanType()),
          new Schema.Field("c", SchemaBuilder.builder.intType()),
        ).asJava,
      ),
    )
    when(record.get("a")).thenReturn("a")
    when(record.get("b")).thenReturn(true)
    when(record.get("c")).thenReturn(1)
    val metadata = RecordMetadata(TopicPartition(new Topic("a"), new Partition(1)), new Offset(100))
    new PrimaryKeysValidator(List("c", "d")).validate(record, metadata) shouldBe Left(
      InvalidInputException(s"Incoming record is missing these primary key(-s):d for record ${metadata.show}"),
    )

    new PrimaryKeysValidator(List("c", "A")).validate(record, metadata) shouldBe Left(
      InvalidInputException(s"Incoming record is missing these primary key(-s):A for record ${metadata.show}"),
    )
  }

  test("validates pks are present") {
    val record = mock[GenericRecord]
    when(record.getSchema).thenReturn(
      Schema.createRecord(
        "r",
        "",
        "ns",
        false,
        List(
          new Schema.Field("a", SchemaBuilder.builder.stringType()),
          new Schema.Field("b", SchemaBuilder.builder.booleanType()),
          new Schema.Field("c", SchemaBuilder.builder.intType()),
        ).asJava,
      ),
    )
    when(record.get("a")).thenReturn("a")
    when(record.get("b")).thenReturn(true)
    when(record.get("c")).thenReturn(1)

    val metadata = RecordMetadata(TopicPartition(new Topic("a"), new Partition(1)), new Offset(100))
    new PrimaryKeysValidator(List("c", "a", "b")).validate(record, metadata) shouldBe Right(())
  }

  test("return an error when at least one of the PK is null") {
    val record = mock[GenericRecord]
    when(record.getSchema).thenReturn(
      Schema.createRecord(
        "r",
        "",
        "ns",
        false,
        List(
          new Schema.Field("a", SchemaBuilder.builder.stringType()),
          new Schema.Field("b", SchemaBuilder.builder.booleanType()),
          new Schema.Field("c", SchemaBuilder.builder.intType()),
        ).asJava,
      ),
    )
    when(record.get("a")).thenReturn("a")
    when(record.get("b")).thenReturn(null)
    when(record.get("c")).thenReturn(null)

    val metadata = RecordMetadata(TopicPartition(new Topic("a"), new Partition(1)), new Offset(100))
    new PrimaryKeysValidator(List("c", "b")).validate(record, metadata) shouldBe Left(
      InvalidInputException(
        s"Incoming record cannot has null for the following primary key(-s):c,b for record ${metadata.show}",
      ),
    )
  }
}
