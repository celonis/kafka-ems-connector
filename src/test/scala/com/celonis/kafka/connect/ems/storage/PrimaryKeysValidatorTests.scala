/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage
import com.celonis.kafka.connect.ems.errors.InvalidInputException
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
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
    validator.validate(record) shouldBe Right(())
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
    validator.validate(record) shouldBe Left(
      InvalidInputException("Incoming record is missing these primary key(-s):d"),
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

    new PrimaryKeysValidator(List("c", "d")).validate(record) shouldBe Left(
      InvalidInputException("Incoming record is missing these primary key(-s):d"),
    )

    new PrimaryKeysValidator(List("c", "A")).validate(record) shouldBe Left(
      InvalidInputException("Incoming record is missing these primary key(-s):A"),
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

    new PrimaryKeysValidator(List("c", "a", "b")).validate(record) shouldBe Right(())
  }
}
