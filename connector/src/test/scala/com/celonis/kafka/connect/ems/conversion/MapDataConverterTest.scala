/*
 * Copyright 2022 Celonis SE
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

package com.celonis.kafka.connect.ems.conversion
import com.celonis.kafka.connect.ems.storage.WorkingDirectory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class MapDataConverterTest extends AnyFunSuite with Matchers with WorkingDirectory {
  private val converter = new org.apache.kafka.connect.json.JsonConverter()
  converter.configure(Map(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG -> "true",
                          "converter.type"                          -> "value",
                          "schemas.enable"                          -> "false",
  ).asJava)
  test("converts a schemaless JSON") {
    val json =
      """
        |{
        |  "idType": 3,
        |  "colorDepth": "",
        |  "threshold" : 45.77,
        |  "evars": {
        |    "evarsa": {
        |      "eVar1": "Tue Aug 27 2019 12:08:10",
        |      "eVar2": 1566922079
        |    }
        |  },
        |  "exclude": {
        |    "id": 0,
        |    "value": false
        |  },
        |  "cars":[ "Ford", "BMW", "Fiat" ],
        |  "nums": [ 1, 3, 4 ]
        |  }
        |}
        |""".stripMargin

    val schemaAndValue = converter.toConnectData("topic", json.getBytes)
    val map = schemaAndValue.value().asInstanceOf[java.util.Map[_, _]]
      .asScala.toMap
    val genericRecord = MapDataConverter.convert(map).getOrElse(fail("Should convert the schemaless json"))
    genericRecord.getSchema.getFields.asScala.map(_.name()).toList shouldBe List(
      "idType",
      "colorDepth",
      "threshold",
      "evars",
      "exclude",
      "cars",
      "nums",
    ).sorted

    genericRecord.get("idType") shouldBe 3
    genericRecord.get("colorDepth") shouldBe ""
    genericRecord.get("threshold") shouldBe 45.77
    val evars = genericRecord.get("evars").asInstanceOf[GenericRecord]
    evars.getSchema.getFields.asScala.map(_.name()).toList.sorted shouldBe List("evarsa")
    val evarsa = evars.get("evarsa").asInstanceOf[GenericRecord]
    evarsa.getSchema.getFields.asScala.map(_.name()).toList.sorted shouldBe List("eVar1", "eVar2")
    evarsa.get("eVar1") shouldBe "Tue Aug 27 2019 12:08:10"
    evarsa.get("eVar2") shouldBe 1566922079L
    val exclude = genericRecord.get("exclude").asInstanceOf[GenericRecord]
    exclude.getSchema.getFields.asScala.map(_.name()).toList.sorted shouldBe List("id", "value")
    exclude.get("id") shouldBe 0
    exclude.get("value") shouldBe false

    genericRecord.get("cars").asInstanceOf[java.util.List[AnyRef]].asScala.toArray shouldBe Array("Ford", "BMW", "Fiat")
    genericRecord.get("nums").asInstanceOf[java.util.List[AnyRef]].asScala.toArray shouldBe Array(1, 3, 4)
  }
}
