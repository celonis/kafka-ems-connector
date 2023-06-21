package com.celonis.kafka.connect.transform.conversion
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

/**
  * Convert Bytes with Logical Type Decimal to Doubles
  */
object DecimalToFloatConversion extends ConnectConversion {
  override def convertSchema(originalSchema: Schema): Schema = originalSchema.name() match {
    case Decimal.LOGICAL_NAME =>
      val newSchema = SchemaBuilder.float64()
      if (originalSchema.isOptional) newSchema.optional()
      newSchema.build()
    case _ => originalSchema
  }

  override def convertValue(connectValue: Any, originalSchema: Schema, targetSchema: Schema): Any =
    (connectValue, originalSchema.`type`(), targetSchema.`type`()) match {
      case (decimal: java.math.BigDecimal, Schema.Type.BYTES, Schema.Type.FLOAT64) => decimal.doubleValue()
      case _                                                                       => connectValue
    }
}
