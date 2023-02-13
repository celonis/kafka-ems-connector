package com.celonis.kafka.connect.transform.fields

import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

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

final case class EmbeddedKafkaMetadata(partition: Int, offset: Long, timestamp: Long) {
  def partitionOffset = s"${partition}_${offset}"
}

object EmbeddedKafkaMetadataFieldInserter extends FieldInserter {
  private val PartitionFieldName       = "kafkaPartition"
  private val OffsetFieldName          = "kafkaOffset"
  private val Timestamp                = "kafkaTimestamp"
  private val PartitionOffsetFieldName = "kafkaPartitionOffset"

  override def insertFields(value: Any, meta: EmbeddedKafkaMetadata): Any =
    value match {
      case value: Struct =>
        val s       = value.schema()
        val fields  = s.fields().asScala.toList
        val nextIdx = fields.map(_.index()).maxOption.map(_ + 1).getOrElse(0)

        val newFields = fields ++ List(
          new Field(PartitionFieldName, nextIdx, Schema.INT32_SCHEMA),
          new Field(OffsetFieldName, nextIdx + 1, Schema.INT64_SCHEMA),
          new Field(Timestamp, nextIdx + 2, Schema.INT64_SCHEMA),
          new Field(PartitionOffsetFieldName, nextIdx + 3, Schema.STRING_SCHEMA),
        )

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
            case _                        => newValue.put(field, value.get(field))
          }
        }
        newValue

      case _ => value
    }
}
