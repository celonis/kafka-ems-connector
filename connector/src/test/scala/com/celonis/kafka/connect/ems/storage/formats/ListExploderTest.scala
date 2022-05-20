/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage.formats

import cats.data.NonEmptySeq
import com.celonis.kafka.connect.ems.storage.SampleData
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.jdk.CollectionConverters.SeqHasAsJava

class ListExploderTest extends AnyFunSuite with Matchers with MockitoSugar with SampleData {

  val arraySchema: Schema = SchemaBuilder.array().items(simpleSchemaV1)

  val containerSchema: Schema = SchemaBuilder.record("myRecord")
    .fields()
    .name("messages").`type`(arraySchema).noDefault()
    .endRecord()

  val struct1: GenericData.Record = buildSimpleStruct()
  val struct2: GenericData.Record = buildSimpleStruct()

  test("explodes a container struct containing array") {

    val explodeArray: GenericData.Array[GenericData.Record] = new GenericData.Array(3, arraySchema)
    explodeArray.add(0, struct1)
    explodeArray.add(1, struct2)

    val arrayContainerStruct: GenericData.Record = new GenericData.Record(containerSchema)
    arrayContainerStruct.put("messages", explodeArray)

    val listExploder = new ListExploder()
    listExploder.explode(arrayContainerStruct) should be(NonEmptySeq.of(struct1, struct2))
    listExploder.explodeSchema(containerSchema) should be(simpleSchemaV1)
  }

  test("explodes a container struct containing list") {

    val explodeArray = List(struct1, struct2).asJava

    val listContainerStruct: GenericData.Record = new GenericData.Record(containerSchema)
    listContainerStruct.put("messages", explodeArray)

    val listExploder = new ListExploder()
    listExploder.explode(listContainerStruct) should be(NonEmptySeq.of(struct1, struct2))
    listExploder.explodeSchema(containerSchema) should be(simpleSchemaV1)
  }

}
