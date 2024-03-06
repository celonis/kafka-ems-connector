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

package com.celonis.kafka.connect.transform.conversion

import com.celonis.kafka.connect.ems.storage.SampleData
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._

import java.math

class DecimalToFloatConversionTest extends AnyFunSuite with Matchers with SampleData {
  test("it does nothing on non-decimal primitive schemas and values") {
    nonDecimalPrimitiveValues.foreach { valueAndSchemas =>
      withClue(valueAndSchemas) {
        conversion.convertSchema(valueAndSchemas.connectSchema) shouldBe valueAndSchemas.connectSchema
        conversion.convert(
          valueAndSchemas.connectValue,
          Some(valueAndSchemas.connectSchema),
        )._1 shouldBe valueAndSchemas.connectValue
      }
    }
  }

  test("it does nothing on non-decimal primitive optional schemas and values") {
    nonDecimalPrimitiveValues.foreach { valueAndSchemas =>
      withClue(valueAndSchemas) {
        conversion.convertSchema(valueAndSchemas.optionalConnectSchema) shouldBe valueAndSchemas.optionalConnectSchema
        conversion.convert(
          valueAndSchemas.connectValue,
          Some(valueAndSchemas.optionalConnectSchema),
        )._1 shouldBe valueAndSchemas.connectValue
        assert(conversion.convert(
          null,
          Some(valueAndSchemas.optionalConnectSchema),
        )._1 == null)
      }
    }
  }

  test("it converts decimal schemas to float64 schemas") {
    conversion.convertSchema(aDecimalSchema) shouldBe Schema.FLOAT64_SCHEMA
  }

  test("it converts decimal values to doubles") {
    conversion.convertValue(math.BigDecimal.valueOf(0.12345), aDecimalSchema, Schema.FLOAT64_SCHEMA) shouldBe 0.12345d
  }

  test("it converts optional decimal values to doubles") {
    conversion.convertValue(
      math.BigDecimal.valueOf(0.12345),
      Decimal.builder(5).optional().schema(),
      SchemaBuilder.float64().optional().schema(),
    ) shouldBe 0.12345d
  }

  test("it is not recursive on structs") {
    val originalSchema = SchemaBuilder.struct().field("a_decimal", aDecimalSchema).build()
    val originalValue  = new Struct(originalSchema)
    originalValue.put("a_decimal", math.BigDecimal.ONE)

    val (convertedValue, Some(convertedSchema)) = conversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe originalSchema
    convertedValue shouldBe originalValue
  }

  test("it is not recursive on maps") {
    val originalSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, aDecimalSchema)
    val originalValue  = Map("a" -> math.BigDecimal.ONE).asJava

    val (convertedValue, Some(convertedSchema)) = conversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe originalSchema
    convertedValue shouldBe originalValue
  }

  test("it is not recursive on lists") {
    val originalSchema = SchemaBuilder.array(aDecimalSchema)
    val originalValue  = List(math.BigDecimal.ONE).asJava

    val (convertedValue, Some(convertedSchema)) = conversion.convert(originalValue, Some(originalSchema))
    convertedSchema shouldBe originalSchema
    convertedValue shouldBe originalValue
  }

  private lazy val nonDecimalPrimitiveValues =
    primitiveValuesAndSchemas.filterNot(_.connectSchema.name() == Decimal.LOGICAL_NAME)

  private lazy val conversion = DecimalToFloatConversion

  private lazy val aDecimalSchema: Schema = Decimal.schema(5)
}
