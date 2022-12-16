package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenConfig
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import cats.syntax.either._

class SchemaFlattenerTest extends org.scalatest.funsuite.AnyFunSuite {
  test("flattens a schema with a nested struct") {
    implicit val config: FlattenConfig = FlattenConfig()
    val schema = SchemaBuilder.struct()
      .field("an_int", SchemaBuilder.int8().build())
      .field("nested", SchemaBuilder.struct().field("a_bool", SchemaBuilder.bool()).build())
      .build()

    val expected: Schema = SchemaBuilder
      .struct()
      .field("an_int", SchemaBuilder.int8().optional().build())
      .field("nested_a_bool", SchemaBuilder.bool().optional().build())
      .build()

    assertResult(expected) {
      SchemaFlattener.flatten(Some(schema)).valueOr(fail(_)).getOrElse(fail("some schema expected"))
    }
  }

  test("drops arrays/maps when discardCollections is set") {
    implicit val config = FlattenConfig().copy(discardCollections = true)

    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a_nested_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build())
      .field("a_nested_array", SchemaBuilder.array(SchemaBuilder.string()).build())
      .field("a_bool", SchemaBuilder.bool().build())
      .build()

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .field("an_array", SchemaBuilder.array(SchemaBuilder.string()).schema())
      .field("a_struct", nestedSchema)
      .build()

    val expected = SchemaBuilder
      .struct()
      .field("a_string", SchemaBuilder.string().optional().build())
      .field("a_struct_a_bool", SchemaBuilder.bool().optional().build())
      .build()

    assertResult(expected) {
      SchemaFlattener.flatten(Some(schema)).valueOr(fail(_)).getOrElse(fail("some schema expected"))
    }
  }

  test("shouldn't flatten a top-level map when discardCollections is set") {
    implicit val config = FlattenConfig().copy(discardCollections = true)

    val schema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().build()).build()
    assertResult(schema) {
      SchemaFlattener.flatten(Some(schema)).valueOr(fail(_)).getOrElse("schema expected")
    }
  }
}
