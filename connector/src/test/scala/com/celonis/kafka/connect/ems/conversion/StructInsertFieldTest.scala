/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.conversion

import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StructInsertFieldTest extends AnyFunSuite with Matchers {
  test("add a new field to a struct") {
    val converter = new AvroData(1)
    val policy    = Policy(1, PolicyOwner("me"), 1.1)
    val record    = Policy.rf.to(policy)
    val input     = converter.toConnectData(SchemaFor[Policy].schema, record)
    val expected  = 1111111L
    val output    = StructInsertField.apply(input.value().asInstanceOf[Struct], OrderFieldInserter.FieldName, expected)
    output.get(OrderFieldInserter.FieldName) shouldBe expected
  }
}

case class PolicyOwner(name: String)

object PolicyOwner {
  implicit val rf: RecordFormat[PolicyOwner] = RecordFormat[PolicyOwner]
}

case class Policy(number: Int, owner: PolicyOwner, amount: Double)

object Policy {
  implicit val rf: RecordFormat[Policy] = RecordFormat[Policy]
}
