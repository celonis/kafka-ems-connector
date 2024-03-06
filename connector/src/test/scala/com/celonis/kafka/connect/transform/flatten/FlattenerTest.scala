/*
 * Copyright 2024 Celonis SE
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

  test("Without a JsonChunk config, a StructFlattener nested in a NormalisingFlattener is used") {
    val config    = FlattenerConfig(false, None)
    val flattener = Flattener.fromConfig(Some(config))

    assert(flattener.isInstanceOf[NormalisingFlattener])
    assert(flattener.asInstanceOf[NormalisingFlattener].innerFlattener.isInstanceOf[StructFlattener])
  }
}
