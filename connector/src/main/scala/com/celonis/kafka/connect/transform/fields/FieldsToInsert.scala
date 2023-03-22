package com.celonis.kafka.connect.transform.fields

sealed trait FieldsToInsert
object FieldsToInsert {
  case object PartitionOffsetTimestamp extends FieldsToInsert
  case object CelonisOrder extends FieldsToInsert
  case object All extends FieldsToInsert
}
