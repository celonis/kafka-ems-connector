package com.celonis.kafka.connect.transform.fields

trait FieldInserter {
  def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any
}
object FieldInserter {
  val noop = new FieldInserter {
    override def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any = value
  }

  def embeddedKafkaMetadata(doInsert: Boolean): FieldInserter =
    if (doInsert)
      EmbeddedKafkaMetadataFieldInserter
    else
      noop
}
