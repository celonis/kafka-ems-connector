package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks

import scala.jdk.CollectionConverters._

class FlattenerConfigTest extends org.scalatest.funsuite.AnyFunSuite {
  test("loads default config from an empty map") {
    val result = FlattenerConfig.apply(Map.empty[String, String].asJava)
    assertResult(FlattenerConfig())(result)
  }
  test("parses dropCollections config") {
    val result = FlattenerConfig.apply(Map("collections.discard" -> "true").asJava)
    assertResult(FlattenerConfig().copy(discardCollections = true))(result)
  }
  test("parses JsonBlobChunks config") {
    val result = FlattenerConfig.apply(
      Map(
        "jsonblob.chunks.max"     -> "3",
        "fallback.varchar.length" -> "65000",
      ).asJava,
    )
    assertResult(FlattenerConfig().copy(jsonBlobChunks = Some(JsonBlobChunks(3, 65000))))(result)
  }
  test("raises an error when jsonblob.chunks.max is supplied without a corresponding fallback varchar length") {
    assertThrows[FlattenerConfig.FallbackVarcharLengthRequired.type](FlattenerConfig.apply(
      Map(
        "jsonblob.chunks.max" -> "3",
      ).asJava,
    ))
  }
}
