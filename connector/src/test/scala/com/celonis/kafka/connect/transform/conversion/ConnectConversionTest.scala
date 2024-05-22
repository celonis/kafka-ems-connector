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

import com.celonis.kafka.connect.transform.PreConversionConfig
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConnectConversionTest extends AnyFunSuite with Matchers {
  test("Build conversion for converting decimals") {
    val conversion =
      ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = true, convertFieldsToLowercase = false))
    conversion shouldBe a[RecursiveConversion]
    conversion.convertSchema(Decimal.schema(5)) shouldBe Schema.FLOAT64_SCHEMA
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe (1d, Some(Schema.FLOAT64_SCHEMA))
  }

  test("Build conversion for converting field names to lowercase") {
    val conversion =
      ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = false, convertFieldsToLowercase = true))
    conversion shouldBe a[RecursiveConversion]

    val schema         = SchemaBuilder.struct().field("UPPER", Timestamp.SCHEMA).build()
    val expectedSchema = SchemaBuilder.struct().field("upper", Timestamp.SCHEMA).build()

    conversion.convertSchema(schema) shouldBe expectedSchema
  }

  test("Build conversion for converting field names to lowercase and decimals to floats") {
    val conversion =
      ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = true, convertFieldsToLowercase = true))
    conversion shouldBe a[RecursiveConversion]

    val schema         = SchemaBuilder.struct().field("DEcimal", Decimal.schema(5)).build()
    val expectedSchema = SchemaBuilder.struct().field("decimal", Schema.FLOAT64_SCHEMA).build()

    conversion.convertSchema(schema) shouldBe expectedSchema
  }

  test("Build noop conversion if all conversions are disabled") {
    val conversion = ConnectConversion.fromConfig(PreConversionConfig(
      convertDecimalsToFloat   = false,
      convertFieldsToLowercase = false,
    ))
    conversion shouldBe ConnectConversion.noOpConversion
    conversion.convertSchema(Decimal.schema(5)) shouldBe Decimal.schema(5)
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe (java.math.BigDecimal.ONE, Some(
      Decimal.schema(5),
    ))
  }
}
