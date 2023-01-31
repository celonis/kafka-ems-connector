package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class FlattenerTest extends AnyFunSuite {
  val config: FlattenerConfig = FlattenerConfig()

  test("do nothing on a primitive") {
    val primitives = Map[Any, Schema](
      123   -> SchemaBuilder.int16().build(),
      "abc" -> SchemaBuilder.string().build(),
      456L  -> SchemaBuilder.int64().build(),
    )

    primitives.foreach {
      case (primitive, schema) =>
        val result = Flattener.flatten(primitive, schema)(config)
        assertResult(result)(primitive)
    }
  }

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
      .field("a_string", SchemaBuilder.string().optional().schema())
      .field("x_a_bool", SchemaBuilder.bool().optional().schema())
      .build()

    val result = Flattener.flatten(struct, schema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("hello")(result.get("a_string"))
    assertResult(true)(result.get("x_a_bool"))

    assertThrows[DataException](result.get("x"))
  }

  test("transforms arrays and maps of primitives into strings") {
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
      .field("nested_an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("nested_a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val mapper = new ObjectMapper()

    val result = Flattener.flatten(struct, schema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())

    assertResult(mutable.Map("1" -> "a", "2" -> "b")) {
      mapper.readValue(result.getString("nested_a_map"), classOf[java.util.Map[String, String]]).asScala
    }
    assertResult(mutable.Buffer(1, 2, 3)) {
      mapper.readValue(result.getString("nested_an_array"), classOf[java.util.LinkedList[String]]).asScala
    }
  }

  test("JSON encodes collection of AVRO records") {
    val nestedSchema = SchemaBuilder.struct()
      .field("a_bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("a_long", Schema.OPTIONAL_INT64_SCHEMA)
      .build()

    val nested = new Struct(nestedSchema)
    nested.put("a_bool", true)
    nested.put("a_long", 33L)

    val schema = SchemaBuilder.struct()
      .field("an_array", SchemaBuilder.array(nestedSchema))
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, nestedSchema))
      .build()

    val struct = new Struct(schema)
    struct.put("an_array", List(nested).asJava)
    struct.put("a_map", Map("key" -> nested).asJava)

    val flatSchema = SchemaBuilder
      .struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val result = Flattener.flatten(struct, schema)(config).asInstanceOf[Struct]
    assertResult("""[{"a_bool":true,"a_long":33}]""")(result.get("an_array"))
    assertResult(flatSchema)(result.schema())
    assertResult("""{"key":{"a_bool":true,"a_long":33}}""")(result.get("a_map"))
  }

  test("JSON encodes collection of JSON records") {

    val nestedSchema = SchemaBuilder.struct()
      .field("a_bool", Schema.BOOLEAN_SCHEMA)
      .field("a_long", Schema.INT64_SCHEMA)
      .build()

    val schema = SchemaBuilder
      .struct()
      .field("an_array", SchemaBuilder.array(nestedSchema).build())
      .field("a_map", SchemaBuilder.map(Schema.STRING_SCHEMA, nestedSchema).build())
      .build()

    val flatSchema = SchemaBuilder
      .struct()
      .field("an_array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("a_map", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val nested = Map[String, Any](
      "a_bool" -> true,
      "a_long" -> 33,
    ).asJava

    val jsonRecord = Map[String, Any](
      "an_array" -> List(nested).asJava,
      "a_map"    -> Map("key" -> nested).asJava,
    ).asJava

    val result = Flattener.flatten(jsonRecord, schema)(config).asInstanceOf[Struct]

    assertResult(flatSchema)(result.schema())
    assertResult("""[{"a_bool":true,"a_long":33}]""")(result.get("an_array"))
    assertResult("""{"key":{"a_bool":true,"a_long":33}}""")(result.get("a_map"))
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
      .field("a_string", SchemaBuilder.string().optional().build())
      .field("a_struct_a_bool", SchemaBuilder.bool().optional().build())
      .build()

    val result = Flattener.flatten(struct, schema)(config).asInstanceOf[Struct]

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

  ignore("serialises a records into multiple JSON chunks when JsonBlobChunks config is set") {
    implicit val config: FlattenerConfig =
      FlattenerConfig().copy(jsonBlobChunks = Some(JsonBlobChunks(chunks = 3, fallbackVarcharLength = 20)))

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", Map("hi" -> "there").asJava)

    // TODO Missing: , ChunkedJsonBlob.schema(config.jsonBlobChunks.get)
    val result = Flattener.flatten(struct, schema).asInstanceOf[Struct]

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
  ignore("raises an error if maxChunks in JsonBlobChunkConfig is insufficient") {
    implicit val config: FlattenerConfig = {
      FlattenerConfig().copy(
        jsonBlobChunks = Some(JsonBlobChunks(
          chunks                = 3,
          fallbackVarcharLength = 2,
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

    // TODO missing: ChunkedJsonBlob.schema(config.jsonBlobChunks.get),
    assertThrows[ChunkedJsonBlob.MisconfiguredJsonBlobMaxChunks](Flattener.flatten(
      struct,
      schema,
    ))
  }

  test("when the schema is inferred, flattens nested maps instead than json-encoding them") {
    val nestedMap = Map(
      "some" -> Map(
        "nested-string" -> "a-string",
        "nested-array"  -> List("a", "b", "c").asJava,
        "nested-map"    -> Map[String, Any]("one-more-level" -> true).asJava,
      ).asJava,
    ).asJava

    val schema = SchemaBuilder.struct()
      .field(
        "some",
        SchemaBuilder.struct()
          .field("nested-string", Schema.OPTIONAL_STRING_SCHEMA)
          .field("nested-array", Schema.OPTIONAL_STRING_SCHEMA)
          .field("nested-map",
                 SchemaBuilder.struct()
                   .field("one-more-level", Schema.OPTIONAL_BOOLEAN_SCHEMA).build(),
          )
          .build(),
      ).build()

    val flattenedSchema = SchemaBuilder.struct()
      .field("some_nested-string", Schema.OPTIONAL_STRING_SCHEMA)
      .field("some_nested-array", Schema.OPTIONAL_STRING_SCHEMA)
      .field("some_nested-map_one-more-level", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .build()

    val expected = new Struct(flattenedSchema)

    expected.put("some_nested-string", "a-string")
    expected.put("some_nested-array", """["a","b","c"]""")
    expected.put("some_nested-map_one-more-level", true)

    assertResult(expected)(Flattener.flatten(nestedMap, schema)(FlattenerConfig()))
  }

}
