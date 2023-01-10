package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

import scala.collection.mutable

class SchemaFlattenerTest extends org.scalatest.funsuite.AnyFunSuite {
  val primitiveFixtures = List(
    1                    -> SchemaBuilder.int8(),
    2                    -> SchemaBuilder.int16(),
    3                    -> SchemaBuilder.int32(),
    4                    -> SchemaBuilder.int64(),
    5.0                  -> SchemaBuilder.float32(),
    6.0                  -> SchemaBuilder.float64(),
    false                -> SchemaBuilder.bool(),
    "hello"              -> SchemaBuilder.string(),
    Array(0x1, 0x0, 0x1) -> SchemaBuilder.bytes(),
  )

  val collectionFixtures = List(
    mutable.HashMap("hello" -> true) -> SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build(),
    List("hello", "world") -> SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
  )

  implicit val config: FlattenerConfig = FlattenerConfig()

  test("flattens a schema making all primitives optional") {

    primitiveFixtures.foreach {
      case (_, primitiveSchema) =>
        val schema = SchemaBuilder.struct()
          .field("a_primitive", primitiveSchema)
          .field(
            "nested",
            SchemaBuilder.struct().field("deeper",
                                         SchemaBuilder.struct().field("a_bool", Schema.BOOLEAN_SCHEMA).schema(),
            ).build(),
          )
          .build()

        val expected: Schema = SchemaBuilder
          .struct()
          .field("a_primitive", primitiveSchema.optional().build())
          .field("nested_deeper_a_bool", SchemaBuilder.bool().optional().build())
          .build()

        withClue(s"expected schema fields ${expected.fields()} for primitive $primitiveSchema") {
          assertResult(expected) {
            SchemaFlattener.flatten(schema)
          }
        }
    }
  }

  test("turns arrays and maps into optional string fields") {
    val schema = SchemaBuilder.struct()
      .field("an_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
      .field(
        "nested",
        SchemaBuilder.struct().field(
          "deeper",
          SchemaBuilder.struct().field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT16_SCHEMA)).schema(),
        ).build(),
      )
      .build()

    val expected = SchemaBuilder.struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field(
        "nested_deeper_a_map",
        Schema.OPTIONAL_STRING_SCHEMA,
      )
      .build()

    assertResult(expected) {
      SchemaFlattener.flatten(schema)
    }
  }

  test("drops arrays/maps when discardCollections is set") {
    implicit val config = FlattenerConfig().copy(discardCollections = true)

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
      SchemaFlattener.flatten(schema)
    }
  }

  test("leaves a top-level collection untouched even when discardCollections is set") {
    implicit val config = FlattenerConfig().copy(discardCollections = true)

    collectionFixtures.foreach {
      case (_, schema) =>
        withClue(s"expected $schema to be unchanged") {
          assertResult(schema) {
            SchemaFlattener.flatten(schema)
          }
        }
    }
  }

  test("generates a schema based on the configured jsonBlobChunks maxChunks value") {
    implicit val config =
      FlattenerConfig().copy(jsonBlobChunks = Some(JsonBlobChunks(chunks = 3, fallbackVarcharLength = 5)))

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .field("an_array", SchemaBuilder.array(SchemaBuilder.string()).schema())
      .build()

    val expected = SchemaBuilder.struct()
      .field("payload_chunk1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("payload_chunk2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("payload_chunk3", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    assertResult(expected) {
      SchemaFlattener.flatten(schema)
    }
  }
}
