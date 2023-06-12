/*
 * Copyright 2023 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

class ChunkedJsonBlobFlattenerTest extends org.scalatest.funsuite.AnyFunSuite {
  test("chunk-encodes hashmaps") {
    val config: JsonBlobChunks = JsonBlobChunks(7, 5)
    val flattener = new ChunkedJsonBlobFlattener(config)
    val nested: java.util.Map[String, Any] = Map("a_bool" -> true.asInstanceOf[Any]).asJava
    val javaMap = Map[String, Any](
      "a_nested_map" -> nested,
    ).asJava

    val connectValue = flattener.flatten(javaMap, None) // Schema is not used by the flattener
    val concatenated = (1 to config.chunks).map(n => connectValue.get(s"payload_chunk$n")).mkString("")

    val om           = new ObjectMapper()
    val expectedJson = om.createObjectNode
    expectedJson.putObject("a_nested_map").put("a_bool", true)

    assertResult(expectedJson)(om.readValue(concatenated, classOf[JsonNode]))
  }

  test("chunk-encodes strings") {
    implicit val config: JsonBlobChunks = JsonBlobChunks(2, 10)
    val flattener    = new ChunkedJsonBlobFlattener(config)
    val someString   = ('a' to 'z').take(10).mkString("")
    val connectValue = flattener.flatten(someString, None)
    val concatenated = (1 to config.chunks).flatMap(n => Option(connectValue.get(s"payload_chunk$n"))).mkString("")
    assertResult(someString)(concatenated)
  }

  test("chunk-encodes structs") {
    val config = JsonBlobChunks(
      chunks                = 3,
      fallbackVarcharLength = 20,
    )

    val flattener = new ChunkedJsonBlobFlattener(config)

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", Map("hi" -> "there").asJava)

    // TODO Missing: , ChunkedJsonBlob.schema(config.jsonBlobChunks.get)
    val result = flattener.flatten(struct, None).asInstanceOf[Struct]

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

  test("generates a schema based on the configured jsonBlobChunks maxChunks value") {
    val config          = JsonBlobChunks(chunks = 3, fallbackVarcharLength = 5)
    val flattener       = new ChunkedJsonBlobFlattener(config)
    val flattenedSchema = flattener.flatten(Map("a" -> 1).asJava, None).schema()

    val expected = SchemaBuilder.struct()
      .field("payload_chunk1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("payload_chunk2", Schema.OPTIONAL_STRING_SCHEMA)
      .field("payload_chunk3", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    assertResult(expected) {
      flattenedSchema
    }
  }

  test("raises an error if maxChunks in JsonBlobChunkConfig is insufficient") {
    val config = JsonBlobChunks(
      chunks                = 3,
      fallbackVarcharLength = 2,
    ) // ^ record byte size will be greater than 3*2 = 6 bytes!

    val flattener = new ChunkedJsonBlobFlattener(config)

    val schema = SchemaBuilder.struct()
      .field("a_string", SchemaBuilder.string().schema())
      .field("a_map", SchemaBuilder.map(SchemaBuilder.string(), SchemaBuilder.string()).schema())
      .build()

    val struct = new Struct(schema)
    struct.put("a_string", "hello")
    struct.put("a_map", Map("hi" -> "there").asJava)

    assertThrows[ChunkedJsonBlobFlattener.MisconfiguredJsonBlobMaxChunks](flattener.flatten(
      struct,
      None,
    ))
  }

  // See https://celonis.atlassian.net/browse/DP-1278
  test("raises an error even when payloadBytes slightly exceeds maxChunks * fallbackVarcharLength") {
    val config     = JsonBlobChunks(chunks = 5, fallbackVarcharLength = 2)
    val someString = "x" * 11 // 11 chunks required
    val flattener  = new ChunkedJsonBlobFlattener(config)

    assertThrows[ChunkedJsonBlobFlattener.MisconfiguredJsonBlobMaxChunks](flattener.flatten(someString, None))
  }

  test("does not raise errors when payloadBytes is exactly maxChunks * fallbackVarcharLength") {
    val config       = JsonBlobChunks(chunks = 5, fallbackVarcharLength = 2)
    val someString   = "x" * 10 // Exactly 5 chunks required
    val flattener    = new ChunkedJsonBlobFlattener(config)
    val connectValue = flattener.flatten(someString, None)
    val concatenated = (1 to config.chunks).flatMap(n => Option(connectValue.get(s"payload_chunk$n"))).mkString("")
    assertResult(someString)(concatenated)
  }

}
