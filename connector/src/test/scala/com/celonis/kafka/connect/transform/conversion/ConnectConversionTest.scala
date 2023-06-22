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
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe 1d
  }

  test("Build noop conversion if convertDecimalsToFloat") {
    val conversion = ConnectConversion.fromConfig(PreConversionConfig(convertDecimalsToFloat = false))
    conversion shouldBe ConnectConversion.noOpConversion
    conversion.convertSchema(Decimal.schema(5)) shouldBe Decimal.schema(5)
    conversion.convert(java.math.BigDecimal.ONE, Some(Decimal.schema(5))) shouldBe java.math.BigDecimal.ONE
  }
}
