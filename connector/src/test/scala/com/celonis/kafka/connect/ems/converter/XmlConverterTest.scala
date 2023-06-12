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

package com.celonis.kafka.connect.ems.converter

import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

class XmlConverterTest extends AnyFunSuite with Matchers {
  test("it should return null schema and value for null input (tombstone messages)") {
    converter.toConnectData("whatever", null) shouldBe SchemaAndValue.NULL
  }

  test("it should convert xml") {
    val xml =
      """
        |<root>
        | <order id="1">
        |   <item>Item 1</item>
        |   <item><inner>Item 2</inner></item>
        |   <different>a</different>
        |   <item>Item 3</item>
        | </order>
        | <order id="2">
        |   <item>Item 1</item>
        |   <item>Item 2</item>
        |   <item>Item 3</item>
        | </order>
        | <other>something else</other>
        |</root>
        |""".stripMargin

    val connectData = converter.toConnectData("topic", xml.getBytes)

    connectData.schema() shouldBe null
    val value = connectData.value().asInstanceOf[java.util.LinkedHashMap[Any, Any]].asScalaRec
    val expected = ListMap(
      "order" -> List(
        ListMap(
          "id"        -> "1",
          "item"      -> List("Item 1", ListMap("inner" -> "Item 2"), "Item 3"),
          "different" -> "a",
        ),
        ListMap(
          "id"   -> "2",
          "item" -> List("Item 1", "Item 2", "Item 3"),
        ),
      ),
      "other" -> "something else",
    )

    value shouldBe expected

  }

  test("it should throw a Connect DataException for deserialisation errors") {
    an[DataException] should be thrownBy (converter.toConnectData(
      "topic",
      "<root>invalid xml</bleah>".getBytes(StandardCharsets.UTF_8),
    ))
  }

  // Recursively transform collections, and preserve Maps traversal order
  implicit class JavaOps(xs: Any) {
    def asScalaRec: Any =
      xs match {
        case xs: java.util.Map[_, _] =>
          val entries = List.from(xs.entrySet().iterator().asScala).map(entry => entry.getKey -> entry.getValue)
          ListMap.from(entries.map { case (k, v) => k -> v.asScalaRec })
        case xs: java.util.Collection[_] => xs.asScala.map(_.asScalaRec)
        case _ => xs
      }

  }

  lazy val converter = new XmlConverter
}
