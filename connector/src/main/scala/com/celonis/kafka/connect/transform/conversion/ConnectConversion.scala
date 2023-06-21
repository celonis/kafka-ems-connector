package com.celonis.kafka.connect.transform.conversion

import org.apache.kafka.connect.data.Schema

trait ConnectConversion {
  def convertSchema(originalSchema: Schema): Schema
  def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any
}

object ConnectConversion {
  val noOpConversion = new ConnectConversion {
    override def convertSchema(originalSchema: Schema): Schema = originalSchema
    override def convertValue(connectValue:    Any, originalSchema: Schema, targetSchema: Schema): Any = connectValue
  }
}
