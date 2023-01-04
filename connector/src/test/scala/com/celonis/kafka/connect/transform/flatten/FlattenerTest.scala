package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.DataException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class FlattenerTest extends AnyFunSuite {
  val config: FlattenerConfig = FlattenerConfig()

  test("flattens a nested field") {

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

    val result = Flattener.flatten(struct, flatSchema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("x_a_bool"))

    assertThrows[DataException](result.get("x"))
  }

  test("transforms arrays and maps into strings") {
    val nestedSchema = SchemaBuilder.struct().name("AStruct")
      .field("an_array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
      .build()

    val nested = new Struct(nestedSchema)
    nested.put("an_array", List(1, 2, 3).asJava)
    nested.put("a_map", Map("1" -> "a", "2" -> "b").asJava)

    val schema = SchemaBuilder.struct()
      .field("nested", nestedSchema)
      .build()

    val struct = new Struct(schema)
    struct.put("nested", nested)

    val flatSchema = SchemaBuilder
      .struct()
      .field("nested_a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .field("nested_an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val mapper = new ObjectMapper()

    val result = Flattener.flatten(struct, flatSchema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())

    assertResult(mutable.Map("1" -> "a", "2" -> "b")) {
      mapper.readValue(result.getString("nested_a_map"), classOf[java.util.Map[String, String]]).asScala
    }
    assertResult(mutable.Buffer(1, 2, 3)) {
      mapper.readValue(result.getString("nested_an_array"), classOf[java.util.LinkedList[String]]).asScala
    }
  }

  test("drops arrays/maps when 'discardCollections' is set") {
    val config: FlattenerConfig = FlattenerConfig().copy(discardCollections = true)

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

    val result = Flattener.flatten(struct, flatSchema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("a_struct_a_bool"))

    assertThrows[DataException](result.get("a_struct"))
    assertThrows[DataException](result.get("a_map"))
    assertThrows[DataException](result.get("an_array"))
  }

  test("leaves top level collections untouched when 'discardCollections' is set") {
    implicit val config: FlattenerConfig = FlattenerConfig().copy(discardCollections = true)
    case class TestData(label: String, value: AnyRef, flattenedSchema: Schema)

    val mapValue:   java.util.Map[String, Int] = mutable.HashMap("x" -> 22).asJava
    val arrayValue: java.util.List[String]     = List("a", "b", "c").asJava

    val testData = List(
      TestData(
        "an map in top-level position",
        mapValue,
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
      ),
      TestData("an array in top-level position", arrayValue, SchemaBuilder.array(Schema.STRING_SCHEMA).build()),
    )
    testData.foreach {
      case TestData(label, value, schema) =>
        withClue(s"$label : $value") {
          assertResult(value) {
            Flattener.flatten(value, schema)
          }
        }
    }
  }

  test("serialises a records into multiple JSON chunks when JsonBlobChunks config is set") {
    implicit val config: FlattenerConfig =
      FlattenerConfig().copy(jsonBlobChunks = Some(JsonBlobChunks(chunkFields = 3, emsVarcharLength = 20)))

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", Map("hi" -> "there").asJava)

    val result =
      Flattener.flatten(struct, ChunkedJsonBlob.schema(config.jsonBlobChunks.get)).asInstanceOf[Struct]

    val om           = new ObjectMapper()
    val expectedJson = om.createObjectNode
    expectedJson.put("a_string", "hello")
    expectedJson.putObject("a_map").put("hi", "there")

    val payload_chunks = (1 to 3).flatMap(n => Option(result.get(s"payload_chunk$n"))).mkString
    val parsedPayload  = om.readValue(payload_chunks, classOf[JsonNode])

    assertResult(expectedJson)(parsedPayload)
    assertResult(List("payload_chunk1", "payload_chunk2", "payload_chunk3"))(
      result.schema().fields().asScala.map(_.name()),
    )
  }
  test("raises an error if maxChunks in JsonBlobChunkConfig is insufficient") {
    implicit val config: FlattenerConfig = {
      FlattenerConfig().copy(
        jsonBlobChunks = Some(JsonBlobChunks(
          chunkFields        = 3,
          emsVarcharLength = 2,
        )), //^ record byte size will be greater than 3*2 = 6 bytes!
      )
    }

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", Map("hi" -> "there").asJava)

    assertThrows[ChunkedJsonBlob.MisconfiguredJsonBlobMaxChunks](Flattener.flatten(
      struct,
      ChunkedJsonBlob.schema(config.jsonBlobChunks.get),
    ))
  }
}
