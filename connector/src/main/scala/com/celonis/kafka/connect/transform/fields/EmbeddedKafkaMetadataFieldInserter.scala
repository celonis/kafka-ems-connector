package com.celonis.kafka.connect.transform.fields

import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

object EmbeddedKafkaMetadataFieldInserter {
  private val PartitionFieldName       = "kafkaPartition"
  private val OffsetFieldName          = "kafkaOffset"
  private val Timestamp                = "kafkaTimestamp"
  private val PartitionOffsetFieldName = "kafkaPartitionOffset"

  // legacy field, kept for backward compatibility with previous connector versions
  private[connect] val CelonisOrderFieldName = "__celonis_order"

  private val PartitionOffsetTimestampFields = Seq(
    PartitionFieldName       -> Schema.INT32_SCHEMA,
    OffsetFieldName          -> Schema.INT64_SCHEMA,
    Timestamp                -> Schema.INT64_SCHEMA,
    PartitionOffsetFieldName -> Schema.STRING_SCHEMA,
  )

  private val CelonisOrderFields = Seq(
    CelonisOrderFieldName -> Schema.INT64_SCHEMA,
  )
}
class EmbeddedKafkaMetadataFieldInserter(fieldsToInsert: FieldsToInsert) extends FieldInserter {
  import EmbeddedKafkaMetadataFieldInserter._

  override def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any =
    value match {
      case value: Struct =>
        val s        = value.schema()
        val fields   = s.fields().asScala.toList
        val startIdx = fields.map(_.index()).maxOption.map(_ + 1).getOrElse(0)

        val additionalFields = fieldsToInsert match {
          case FieldsToInsert.PartitionOffsetTimestamp =>
            PartitionOffsetTimestampFields
          case FieldsToInsert.CelonisOrder =>
            CelonisOrderFields
          case FieldsToInsert.All =>
            PartitionOffsetTimestampFields ++ CelonisOrderFields
        }

        val newFields = fields ++ fieldsWithIndex(startIdx)(additionalFields: _*)

        val newSchema = new ConnectSchema(s.`type`(),
                                          s.isOptional,
                                          s.defaultValue(),
                                          s.name(),
                                          s.version(),
                                          s.doc(),
                                          s.parameters(),
                                          newFields.asJava,
                                          null,
                                          null,
        )
        val newValue = new Struct(newSchema)
        for (field <- newSchema.fields().asScala) {
          field.name() match {
            case PartitionFieldName       => newValue.put(PartitionFieldName, meta.partition)
            case OffsetFieldName          => newValue.put(OffsetFieldName, meta.offset)
            case Timestamp                => newValue.put(Timestamp, meta.timestamp)
            case PartitionOffsetFieldName => newValue.put(PartitionOffsetFieldName, meta.partitionOffset)
            case CelonisOrderFieldName    => newValue.put(CelonisOrderFieldName, meta.offset)
            case _                        => newValue.put(field, value.get(field))
          }
        }
        newValue

      case _ => value
    }

  private def fieldsWithIndex(startIdx: Int)(fieldSchemas: (String, Schema)*): Seq[Field] =
    LazyList.from(startIdx).zip(fieldSchemas).map {
      case (idx, (fieldName, schema)) => new Field(fieldName, idx, schema)
    }
}
