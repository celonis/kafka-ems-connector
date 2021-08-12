/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

case class RecordMetadata(topicPartition: TopicPartition, offset: Offset)
case class Record(key: Option[SinkData], value: SinkData, metadata: RecordMetadata)

sealed trait SinkData {
  def schema(): Schema
}

sealed trait PrimitiveSinkData extends SinkData {
  def value: Any
}

case class BooleanSinkData(value: Boolean) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.bool().schema()
}

case class StringSinkData(value: String) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.string().schema()
}

case class LongSinkData(value: Long) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.int64().schema()
}

case class IntSinkData(value: Int) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.int32().schema()
}

case class ByteSinkData(value: Byte) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.int8().schema()
}

case class DoubleSinkData(value: Double) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.float64().schema()
}

case class FloatSinkData(value: Float) extends PrimitiveSinkData {
  override def schema(): Schema = SchemaBuilder.float32().schema()
}

case class StructSinkData(struct: Struct) extends SinkData {
  override def schema(): Schema = struct.schema()
}

case class MapSinkData(map: Map[SinkData, SinkData], schema: Schema) extends SinkData

case class ArraySinkData(array: Seq[SinkData], schema: Schema) extends SinkData

case class ByteArraySinkData(array: Array[Byte]) extends SinkData {
  override def schema(): Schema = SchemaBuilder.array(SchemaBuilder.int8()).schema()
}

case class NullSinkData(schema: Schema) extends SinkData {}
