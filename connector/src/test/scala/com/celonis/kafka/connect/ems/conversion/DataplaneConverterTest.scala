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

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.confluent.connect.avro.AvroData
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import java.time.ZoneOffset
import java.util.Date

class DataplaneConverterTest extends AsyncFunSuite with AsyncIOSpec with Matchers {

  val parser = new Schema.Parser()
  import parser.parse
  val personAvroSchema =
    parse(
      """
        |{
        |  "name":"person",
        |  "type":"record",
        |  "fields":[
        |    {
        |      "name":"firstname",
        |      "type":"string"
        |    },
        |    {
        |      "name":"favoritePet",
        |      "type":{
        |        "type":"enum",
        |        "name":"enumName",
        |        "symbols":[
        |          "DOG",
        |          "CAT"
        |        ]
        |      }
        |    },
        |    {
        |      "name":"birthdate",
        |      "default":null,
        |      "type":[
        |        "null",
        |        {
        |          "type":"int",
        |          "logicalType":"date"
        |        }
        |      ]
        |    }
        |  ]
        |}
        |""".stripMargin,
    )

  private val avroData = new AvroData(DataplaneConverter.avroDataConfig)
  import avroData.toConnectSchema
  val personConnectSchema = toConnectSchema(personAvroSchema)

  test("DataplaneConverter.toAvroMessage - encodes struct to base64 encoded AVRO") {
    val birthdate = LocalDate.parse("1970-01-02")
    val inputValue =
      structFor(personConnectSchema)(
        ("firstname", "Simon"),
        ("favoritePet", "CAT"),
        ("birthdate", Date.from(birthdate.atStartOfDay(ZoneOffset.UTC).toInstant)),
      )

    val expectedRecord =
      recordFor(personAvroSchema)(
        ("firstname", "Simon"),
        ("favoritePet", new GenericData.EnumSymbol(personAvroSchema.getField("favoritePet").schema(), "CAT")),
        ("birthdate", birthdate.toEpochDay.toInt: java.lang.Integer),
      )

    for {
      c             <- DataplaneConverter.toAvroMessage[IO](None, personAvroSchema)
      msg           <- c.convert(new SinkRecord("topic", 0, null, null, personConnectSchema, inputValue, 0))
      expectedValue <- DataplaneConverter.base64Avro[IO](expectedRecord, personAvroSchema)
    } yield {
      assert(msg.key == None)
      assert(msg.value == Some(expectedValue))
    }

  }

  test("DataplaneConverter.toAvroMessage - encodes top-level STRING to base64 encoded AVRO") {
    val inputValue = "string"

    val avroSchema    = parse("""{ "type": "string" }""")
    val connectSchema = toConnectSchema(avroSchema)

    for {
      c             <- DataplaneConverter.toAvroMessage[IO](None, avroSchema)
      msg           <- c.convert(new SinkRecord("topic", 0, null, null, connectSchema, inputValue, 0))
      expectedValue <- DataplaneConverter.base64Avro[IO](inputValue, avroSchema)
    } yield {
      assert(msg.key == None)
      assert(msg.value == Some(expectedValue))
    }
  }

  private def structFor(schema: ConnectSchema)(fields: (String, AnyRef)*): Struct = {
    val s = new Struct(schema)
    fields.foreach { case (f, v) => s.put(f, v) }
    s
  }

  private def recordFor(schema: Schema)(fields: (String, AnyRef)*): GenericRecord =
    fields.foldLeft(new GenericRecordBuilder(schema)) { case (b, (f, v)) => b.set(f, v) }
      .build()

}
