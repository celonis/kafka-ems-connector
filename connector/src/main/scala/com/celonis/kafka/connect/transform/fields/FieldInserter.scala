package com.celonis.kafka.connect.transform.fields

trait FieldInserter {
  def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any
}
object FieldInserter {
  val noop = new FieldInserter {
    override def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any = value
  }

  def embeddedKafkaMetadata(doInsert: Boolean, configuredOrderField: Option[String]): FieldInserter =
    (doInsert, configuredOrderField.contains(EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName)) match {
      case (true, true)   => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.All)
      case (true, false)  => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.PartitionOffsetTimestamp)
      case (false, true)  => new EmbeddedKafkaMetadataFieldInserter(FieldsToInsert.CelonisOrder)
      case (false, false) => noop
    }
}
