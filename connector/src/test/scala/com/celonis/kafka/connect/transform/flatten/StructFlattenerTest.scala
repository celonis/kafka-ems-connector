package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.{FlattenConfig, LeafNode}
import org.apache.kafka.connect.data.{Date, Schema, SchemaBuilder, Struct}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, ZoneId}

class StructFlattenerTest extends AnyFunSuite with Matchers  {

  implicit val config = FlattenConfig()

  test("should handle null struct") {
    StructFlattener.flatten(Seq("some", "path"), null) should be(Seq.empty)
  }

  test("should handle struct with null field") {

    val someSchema: Schema = SchemaBuilder.struct()
      .field("caseId", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val someStruct: Struct = new Struct(someSchema)

    StructFlattener.flatten(Seq("some", "path"), someStruct) should be(Seq(LeafNode(List("some", "path", "caseId"),
                                                                                    null,
    )))
  }

  test("should handle struct with null struct inside") {

    val someInnerSchema: Schema = SchemaBuilder.struct().optional()
      .build()

    val someSchema: Schema = SchemaBuilder.struct()
      .field("innie", someInnerSchema)
      .build()

    val someStruct: Struct = new Struct(someSchema)

    StructFlattener.flatten(Seq("some", "path"), someStruct) should be(Seq.empty)
  }

  // a Date data type should be wrapped in a Kafka Connect logical type
  test("should handle struct with date field") {

    val dateSchema = Date.SCHEMA

    val someSchema: Schema = SchemaBuilder.struct()
      .field("date", dateSchema)
      .build()

    val someDate = java.util.Date.from(LocalDate.now().atStartOfDay()
      .atZone(ZoneId.systemDefault())
      .toInstant)

    val someStruct: Struct = new Struct(someSchema)
    someStruct.put("date", someDate)

    val flatHead = StructFlattener.flatten(Seq("some", "path"), someStruct).head
    flatHead.path shouldBe Seq("some", "path", "date")
    flatHead.value shouldBe someDate
  }
}
