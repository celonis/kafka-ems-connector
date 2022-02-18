/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import cats.data.NonEmptySeq
import org.apache.avro.generic.GenericRecord
import org.scalatestplus.mockito.MockitoSugar

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NoOpExploderTest extends AnyFunSuite with Matchers with MockitoSugar {

  test("returns a singleton list") {
    val genericRecord = mock[GenericRecord]
    val noOpExploder  = new NoOpExploder()
    noOpExploder.explode(genericRecord) should be(NonEmptySeq.one(genericRecord))
  }

}
