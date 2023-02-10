package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import org.scalatest.funsuite.AnyFunSuite

class FlattenerTest extends AnyFunSuite {
  test("Without a config, a noOp flattener is used") {
    val flattener = Flattener.fromConfig(None)

    assertResult(Flattener.noOpFlattener)(flattener)
  }

  test("With a JsonChunk config, a JsonChunkFlattener is used") {
    val config    = FlattenerConfig(false, Some(JsonBlobChunks(1, 1)))
    val flattener = Flattener.fromConfig(Some(config))

    assert(flattener.isInstanceOf[ChunkedJsonBlobFlattener])
  }

  test("Without a JsonChunk config, a StructFlattener is used") {
    val config = FlattenerConfig(false, None)
    val flattener = Flattener.fromConfig(Some(config))

    assert(flattener.isInstanceOf[StructFlattener])
  }
}
