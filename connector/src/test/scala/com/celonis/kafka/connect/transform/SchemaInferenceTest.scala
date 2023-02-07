package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.transform.SchemaInference.ValueAndSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters._

class SchemaInferenceTest extends org.scalatest.funsuite.AnyFunSuite {
  test("returns None when encountering an unexpected value") {
    List[Any](
      null,
      Range(1, 10),
      Iterator.continually(true),
      (),
    ).foreach {
      case value =>
        assertResult(None)(SchemaInference(value))
    }
  }
  test("Infers the schema of simple primitives") {
    List(
      "hi"  -> Schema.OPTIONAL_STRING_SCHEMA,
      12L   -> Schema.OPTIONAL_INT64_SCHEMA,
      15.2d -> Schema.OPTIONAL_FLOAT64_SCHEMA,
      true  -> Schema.OPTIONAL_BOOLEAN_SCHEMA,
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(expectedSchema))(inferSchema(value))
    }
  }

  test("Infers simple maps of strings") {
    val value          = Map("hi" -> "there").asJava
    val expectedSchema = SchemaBuilder.struct().field("hi", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedValue  = new Struct(expectedSchema).put("hi", "there")

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(SchemaInference(value))
  }

  test("Infers simple maps of primitives") {
    val value          = Map(1L -> true).asJava
    val expectedSchema = SchemaBuilder.struct().field("1", Schema.OPTIONAL_BOOLEAN_SCHEMA).build()
    val expectedValue  = new Struct(expectedSchema).put("1", true)

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(SchemaInference(value))
  }

  test("Infers simple collections") {
    val value          = List("a", "b", "c").asJava
    val expectedSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build()

    assertResult(Some(ValueAndSchema(value, expectedSchema)))(SchemaInference(value))
  }

  test("Normalisation transforms maps nested in maps") {
    val value          = Map("nested" -> Map("a" -> "123").asJava).asJava
    val nestedSchema   = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedSchema = SchemaBuilder.struct().field("nested", nestedSchema).build()
    val expectedValue  = new Struct(expectedSchema).put("nested", new Struct(nestedSchema).put("a", "123"))

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(SchemaInference(value))
  }

  test("Normalisation transforms maps nested in arrays") {
    val value          = List(Map("a" -> "123").asJava).asJava
    val nestedSchema   = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build()
    val expectedSchema = SchemaBuilder.array(nestedSchema).build()
    val expectedValue  = List(new Struct(nestedSchema).put("a", "123")).asJava

    assertResult(Some(ValueAndSchema(expectedValue, expectedSchema)))(SchemaInference(value))
  }

  test("Infers heterogeneous collections as byte collections") {
    List(
      Map.empty[Boolean, Boolean].asJava -> SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build(),
      List.empty[Int].asJava             -> SchemaBuilder.array(Schema.BYTES_SCHEMA).build(),
      List(1, "blah", true).asJava       -> SchemaBuilder.array(Schema.BYTES_SCHEMA).build(),
    ).foreach {
      case (value, expectedSchema) =>
        assertResult(Some(ValueAndSchema(value, expectedSchema)))(SchemaInference(value))
    }
  }

  test("infers nested object's schema") {
    val rawJson =
      """
        |{"hello": {"I": "am_a_nested", "object": true, "with": ["an","array", "of-strings"]}}
        |""".stripMargin
    val om     = new ObjectMapper()
    val value  = om.readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val schema = inferSchema(value).getOrElse(fail("some schema expected!"))

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
    val om = new ObjectMapper()
    val value = om.readValue(rawJson, classOf[java.util.Map[String, AnyRef]])
    val schema = inferSchema(value).getOrElse(fail("some schema expected!"))

    assertResult(
      SchemaBuilder.struct().field(
        "hello",
        SchemaBuilder.struct()
          .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .build(),
      ).build(),
    )(schema)
  }

  private def inferSchema(value: Any): Option[Schema] =
    SchemaInference(value).map(_.schema)
}
