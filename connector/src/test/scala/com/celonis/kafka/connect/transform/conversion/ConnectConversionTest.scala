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
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConnectConversionTest extends AnyFunSuite with Matchers {
  test("Build conversion from config") {
    val conversion = ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = true))
    conversion shouldBe a[RecursiveConversion]
    conversion.convertSchema(Decimal.schema(5)) shouldBe Schema.FLOAT64_SCHEMA
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe (1d, Some(Schema.FLOAT64_SCHEMA))
  }

  test("Build noop conversion if convertDecimalsToFloat") {
    val conversion = ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = false))
    conversion shouldBe ConnectConversion.noOpConversion
    conversion.convertSchema(Decimal.schema(5)) shouldBe Decimal.schema(5)
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe (java.math.BigDecimal.ONE, Some(
      Decimal.schema(5),
    ))
  }
}
