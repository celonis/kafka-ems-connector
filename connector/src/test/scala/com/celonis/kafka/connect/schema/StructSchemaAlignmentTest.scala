package com.celonis.kafka.connect.schema

import com.celonis.kafka.connect.schema.StructSchemaAlignmentTest.Scenario
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.Inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

class StructSchemaAlignmentTest extends AnyFunSuite with Matchers with Inside {
  for (
    scenario <- Seq(
      Scenario.nestedStruct,
      Scenario.structInArray,
      Scenario.nestedArray,
      Scenario.nestedMap,
      Scenario.structInMap,
    )
  ) {
    test(scenario.label) {
      StructSchemaAlignment.alignTo(scenario.superSchema, scenario.inputValue) shouldEqual scenario.expectedAlignedValue
    }
  }
}
object StructSchemaAlignmentTest {
  final case class Scenario(label: String, superSchema: Schema, inputValue: Struct, expectedAlignedValue: Struct) {
    override def toString: String = label
  }

  object Scenario {
    val nestedStruct: Scenario = {
      val superSchema: Schema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).field("b2", Schema.OPTIONAL_INT64_SCHEMA).build,
      ).build

      val subSchema: Schema = SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).build

      val schema: Schema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field("b", subSchema).build

      val struct    = new Struct(schema)
      val subStruct = new Struct(subSchema)

      subStruct.put("b1", "hello")
      struct.put("a", 1L)
      struct.put("b", subStruct)

      val expected  = new Struct(superSchema)
      val expectedB = new Struct(superSchema.field("b").schema)
      expectedB.put("b1", "hello")

      expected.put("a", 1L)
      expected.put("b", expectedB)

      Scenario(
        label                = "aligns nested struct values to the super-schema",
        superSchema          = superSchema,
        inputValue           = struct,
        expectedAlignedValue = expected,
      )
    }

    val structInArray: Scenario = {
      val superSchema: Schema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.array(SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).field("b2",
                                                                                         Schema.OPTIONAL_INT64_SCHEMA,
        ).build).build,
      ).build

      val subSchema: Schema = SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).build

      val schema: Schema =
        SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field("b", SchemaBuilder.array(subSchema).build).build

      val struct:    Struct = new Struct(schema)
      val subStruct: Struct = new Struct(subSchema)

      subStruct.put("b1", "hello")
      struct.put("a", 1L)
      struct.put("b", List(subStruct).asJava)

      val expected:  Struct = new Struct(superSchema)
      val expectedB: Struct = new Struct(superSchema.field("b").schema.valueSchema)
      expectedB.put("b1", "hello")

      expected.put("a", 1L)
      expected.put("b", List(expectedB).asJava)

      Scenario(
        label                = "aligns arrays of structs to the super-schema",
        superSchema          = superSchema,
        inputValue           = struct,
        expectedAlignedValue = expected,
      )
    }

    val nestedArray: Scenario = {
      val superSchema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA).build).build,
      ).build

      val subSchema = SchemaBuilder.struct.field("b", superSchema.field("b").schema)

      val struct = new Struct(subSchema)
      struct.put("b", List(List("a", "b", "c").asJava, List("x", "y", "z").asJava).asJava)

      val expected = new Struct(superSchema)
      expected.put("b", struct.getArray("b"))

      Scenario(
        label                = "aligns nested arrays to the super-schema",
        superSchema          = superSchema,
        inputValue           = struct,
        expectedAlignedValue = expected,
      )
    }

    val nestedMap: Scenario = {
      val superSchema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.map(SchemaBuilder.string, SchemaBuilder.array(Schema.STRING_SCHEMA).build).build,
      ).build

      val subSchema = SchemaBuilder.struct.field("b", superSchema.field("b").schema)

      val struct = new Struct(subSchema)
      struct.put("b", Map("abc" -> List("a", "b", "c").asJava, "xyz" -> List("x", "y", "z").asJava).asJava)

      val expected = new Struct(superSchema)
      expected.put("b", struct.getMap("b"))

      Scenario(
        label                = "aligns an array nested into a map to the super-schema",
        superSchema          = superSchema,
        inputValue           = struct,
        expectedAlignedValue = expected,
      )
    }

    val structInMap: Scenario = {
      val superSchema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.map(
          SchemaBuilder.struct.field("b0", Schema.STRING_SCHEMA).build,
          SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).field("b2", Schema.OPTIONAL_INT64_SCHEMA).build,
        ).build,
      ).build

      val keySubSchema   = SchemaBuilder.struct.field("b0", Schema.STRING_SCHEMA).build
      val valueSubSchema = SchemaBuilder.struct.field("b1", Schema.STRING_SCHEMA).build

      val schema = SchemaBuilder.struct.field("a", Schema.INT64_SCHEMA).field(
        "b",
        SchemaBuilder.map(keySubSchema, valueSubSchema).build,
      ).build

      val struct       = new Struct(schema)
      val keySubStruct = new Struct(keySubSchema)
      keySubStruct.put("b0", "hello-key")

      val valueSubStruct = new Struct(valueSubSchema)
      valueSubStruct.put("b1", "hello-key")

      struct.put("a", 1L)
      struct.put("b", Map(keySubStruct -> valueSubStruct).asJava)

      val expected = new Struct(superSchema)

      val expectedBKey = new Struct(superSchema.field("b").schema.keySchema)
      expectedBKey.put("b0", keySubStruct.get("b0"))
      val expectedBValue = new Struct(superSchema.field("b").schema.valueSchema)
      expectedBValue.put("b1", valueSubStruct.get("b1"))

      expected.put("a", 1L)
      expected.put("b", Map(expectedBKey -> expectedBValue).asJava)

      Scenario(
        label                = "aligns maps of structs to the super-schema",
        superSchema          = superSchema,
        inputValue           = struct,
        expectedAlignedValue = expected,
      )
    }

  }

}
