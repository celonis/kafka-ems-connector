package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import scala.jdk.CollectionConverters._

class ChunkedJsonBlobFlattenerTest extends org.scalatest.funsuite.AnyFunSuite {
  test("asConnectData encodes hashmaps as chunked JSON payload structs") {
    implicit val config: JsonBlobChunks             = JsonBlobChunks(7, 5)
    val nested:          java.util.Map[String, Any] = Map("a_bool" -> true.asInstanceOf[Any]).asJava
    val javaMap = Map[String, Any](
      "a_nested_map" -> nested,
    ).asJava

    val connectValue = ChunkedJsonBlobFlattener.asConnectData(javaMap)
    val concatenated = (1 to config.chunks).map(n => connectValue.get(s"payload_chunk$n")).mkString("")

    val om           = new ObjectMapper()
    val expectedJson = om.createObjectNode
    expectedJson.putObject("a_nested_map").put("a_bool", true)

    assertResult(expectedJson)(om.readValue(concatenated, classOf[JsonNode]))
  }

  test("asConnectData chunk-encodes strings") {
    implicit val config: JsonBlobChunks = JsonBlobChunks(2, 10)
    val someString   = ('a' to 'z').take(10).mkString("")
    val connectValue = ChunkedJsonBlobFlattener.asConnectData(someString)
    val concatenated = (1 to config.chunks).flatMap(n => Option(connectValue.get(s"payload_chunk$n"))).mkString("")
    assertResult(someString)(concatenated)
  }
}
