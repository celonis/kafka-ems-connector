package com.celonis.kafka.connect.transform.conversion

import com.celonis.kafka.connect.transform.PreConversionConfig
import org.apache.kafka.connect.data.Schema

trait ConnectConversion {
  def convertSchema(originalSchema: Schema): Schema
  def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any
  final def convert(value: Any, originalSchema: Option[Schema]): (Any, Option[Schema]) =
    originalSchema match {
      case Some(originalSchema) =>
        val targetSchema = convertSchema(originalSchema)
        (convertValue(value, originalSchema, targetSchema), Some(targetSchema))
      case None => (value, None)
    }
}

object ConnectConversion {
  def fromConfig(config: PreConversionConfig): ConnectConversion =
    if (config.convertDecimalsToFloat) new RecursiveConversion(DecimalToFloatConversion)
    else noOpConversion

  val noOpConversion: ConnectConversion = new ConnectConversion {
    override def convertSchema(originalSchema: Schema): Schema = originalSchema
    override def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any = connectValue
  }
}
