package com.celonis.kafka.connect.transform

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper
import scala.jdk.CollectionConverters._

class SchemaInferenceTest extends org.scalatest.funsuite.AnyFunSuite {
  test("Infers the schema of simple primitives") {
    val nullValue: Any = null
    List(
      "hi"      -> Schema.OPTIONAL_STRING_SCHEMA,
      12L       -> Schema.OPTIONAL_INT64_SCHEMA,
      15.2d     -> Schema.OPTIONAL_FLOAT64_SCHEMA,
      true      -> Schema.OPTIONAL_BOOLEAN_SCHEMA,
      nullValue -> Schema.BYTES_SCHEMA, //sentinel value to represent nulls, omitted unless at the top level
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(expectedSchema))(SchemaInference(value))
    }
  }

  test("Infers non-empty collections") {
    List(
      Map("hi" -> "there").asJava -> SchemaBuilder.struct().field("hi", Schema.OPTIONAL_STRING_SCHEMA).build(),
      Map(1L -> true).asJava -> SchemaBuilder.struct().field("1", Schema.OPTIONAL_BOOLEAN_SCHEMA).build(),
      List("a", "b", "c").asJava -> SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build(),
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(expectedSchema))(SchemaInference(value))
    }
  }

  test("Infers heterogeneous collections as byte collections") {
    List(
      Map.empty[Boolean, Boolean].asJava -> SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build(),
      List.empty[Int].asJava             -> SchemaBuilder.array(Schema.BYTES_SCHEMA).build(),
      List(1, "blah", true).asJava       -> SchemaBuilder.array(Schema.BYTES_SCHEMA).build(),
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(expectedSchema))(SchemaInference(value))
    }
  }

  test("infers nested object's schema") {
    val rawJson =
      """
        |{"hello": {"I": "am_a_nested", "object": true, "with": ["an","array", "of-strings"]}}
        |""".stripMargin
    val om     = new ObjectMapper()
    val value  = om.readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val schema = SchemaInference(value).getOrElse(fail("some schema expected!"))

    assertResult(
      SchemaBuilder.struct().field(
        "hello",
        SchemaBuilder.struct()
          .field("I", Schema.OPTIONAL_STRING_SCHEMA)
          .field("object", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("with", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
          .build(),
      ).build(),
    )(schema)
  }

  test("omits fields with a null value") {
    val rawJson =
      """
        |{"hello": {"f1": true, "omit_me_iam_a_null_value": null}}
        |""".stripMargin
    val om     = new ObjectMapper()
    val value  = om.readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val schema = SchemaInference(value).getOrElse(fail("some schema expected!"))

    assertResult(
      SchemaBuilder.struct().field(
        "hello",
        SchemaBuilder.struct()
          .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .build(),
      ).build(),
    )(schema)

  }
}
