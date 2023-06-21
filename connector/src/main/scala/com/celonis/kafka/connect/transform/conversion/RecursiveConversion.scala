package com.celonis.kafka.connect.transform.conversion
import com.celonis.kafka.connect.transform.conversion.RecursiveConversion.SchemaBuilderOps
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters._

final class RecursiveConversion(innerConversion: ConnectConversion) extends ConnectConversion {

  override def convertSchema(originalSchema: Schema): Schema =
    originalSchema.`type`() match {
      case Schema.Type.STRUCT =>
        originalSchema.fields().asScala.foldLeft(SchemaBuilder.struct()) { case (builder, field) =>
          builder.field(field.name(), convertSchema(field.schema()))
        }.optionalIf(originalSchema.isOptional).build()
      case Schema.Type.ARRAY =>
        SchemaBuilder.array(convertSchema(originalSchema.valueSchema())).optionalIf(originalSchema.isOptional).build()
      case Schema.Type.MAP =>
        SchemaBuilder.map(
          convertSchema(originalSchema.keySchema()),
          convertSchema(originalSchema.valueSchema()),
        ).optionalIf(originalSchema.isOptional).build()
      case _ => innerConversion.convertSchema(originalSchema)
    }

  override def convertValue(connectValue: Any, originalSchema: Schema, targetSchema: Schema): Any =
    connectValue match {
      case connectValue: Struct =>
        val newStruct = new Struct(targetSchema)
        targetSchema.fields().asScala.foreach { field =>
          newStruct.put(
            field.name(),
            convertValue(connectValue.get(field), originalSchema.field(field.name()).schema(), field.schema()),
          )
        }
        newStruct
      case connectValue: java.util.Map[_, _] =>
        connectValue.asScala.map {
          case (key, value) =>
            convertValue(key, originalSchema.keySchema(), targetSchema.keySchema()) ->
              convertValue(value, originalSchema.valueSchema(), targetSchema.valueSchema())
        }.toMap.asJava
      case connectValue: java.util.List[_] =>
        connectValue.asScala.map {
          value => convertValue(value, originalSchema.valueSchema(), targetSchema.valueSchema())
        }.asJava
      case other => innerConversion.convertValue(other, originalSchema, targetSchema)

    }
}

object RecursiveConversion {
  implicit class SchemaBuilderOps(builder: SchemaBuilder) {
    def optionalIf(condition: Boolean): SchemaBuilder =
      if (condition) builder.optional() else builder
  }
}
