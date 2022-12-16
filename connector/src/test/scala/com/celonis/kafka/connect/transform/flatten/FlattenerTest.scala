package com.celonis.kafka.connect.transform.flatten

import cats.syntax.either._
import com.celonis.kafka.connect.transform.FlattenConfig
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class FlattenerTest extends AnyFunSuite {
  test("flattens a nested field") {
    val config = FlattenConfig().copy(discardCollections = true)

    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a_bool", SchemaBuilder.bool().build())
      .build()
    val nested = new Struct(nestedSchema)
    nested.put("a_bool", true)

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("x", nestedSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("x", nested)

    val flatSchema = SchemaBuilder
      .struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("x_a_bool", SchemaBuilder.bool().schema())
      .build()

    val result = Flattener.flatten(struct, Some(flatSchema))(config).valueOr(fail(_)).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("x_a_bool"))

    assertThrows[DataException](result.get("x"))
  }

  test("drops arrays/maps when discardCollections is set") {
    val config = FlattenConfig().copy(discardCollections = true)

    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("a_nested_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).build())
      .field("a_nested_array", SchemaBuilder.array(SchemaBuilder.string()).build())
      .field("a_bool", SchemaBuilder.bool().build())
      .build()
    val nested = new Struct(nestedSchema)
    nested.put("a_nested_map", mutable.HashMap("x" -> "y").asJava)
    nested.put("a_nested_array", List("blah").asJava)
    nested.put("a_bool", true)

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .field("an_array", SchemaBuilder.array(SchemaBuilder.string()).schema())
      .field("a_struct", nestedSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", mutable.HashMap("hello" -> "hi-there...").asJava)
    struct.put("an_array", List("discard", "me", "please").asJava)
    struct.put("a_struct", nested)

    val flatSchema = SchemaBuilder
      .struct()
      .field("a_string", schema.field("a_string").schema())
      .field("a_struct_a_bool", nestedSchema.field("a_bool").schema())
      .build()

    val result = Flattener.flatten(struct, Some(flatSchema))(config).valueOr(fail(_)).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("a_struct_a_bool"))

    assertThrows[DataException](result.get("a_struct"))
    assertThrows[DataException](result.get("a_map"))
    assertThrows[DataException](result.get("an_array"))
  }
}
