package com.celonis.kafka.connect.transform.fields

import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

trait FieldInserter[C] {
  def insertFields(value: Any, context: C): Any
}
object FieldInserter {
  def noop[T] = new FieldInserter[T] {
    override def insertFields(value: Any, context: T): Any = value
  }

  def partitionOffset(doInsert: Boolean): FieldInserter[PartitionOffset] =
    if (doInsert)
      PartitionOffsetFieldInserter
    else
      noop
}

final case class PartitionOffset(partition: Int, offset: Long) {
  def asString = s"${partition}_${offset}"
}

object PartitionOffsetFieldInserter extends FieldInserter[PartitionOffset] {
  private val PartitionFieldName       = "partition"
  private val OffsetFieldName          = "offset"
  private val PartitionOffsetFieldName = "partitionOffset"

  override def insertFields(value: Any, context: PartitionOffset): Any =
    value match {
      case value: Struct =>
        val s       = value.schema()
        val fields  = s.fields().asScala.toList
        val nextIdx = fields.map(_.index()).maxOption.map(_ + 1).getOrElse(0)

        val newFields = fields ++ List(
          new Field(PartitionFieldName, nextIdx, Schema.INT32_SCHEMA),
          new Field(OffsetFieldName, nextIdx + 1, Schema.INT64_SCHEMA),
          new Field(PartitionOffsetFieldName, nextIdx + 2, Schema.STRING_SCHEMA),
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
            case PartitionFieldName       => newValue.put(PartitionFieldName, context.partition)
            case OffsetFieldName          => newValue.put(OffsetFieldName, context.offset)
            case PartitionOffsetFieldName => newValue.put(PartitionOffsetFieldName, context.asString)
            case _                        => newValue.put(field, value.get(field))
          }
        }
        newValue

      case _ => value
    }
}
