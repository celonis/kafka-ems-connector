package com.celonis.kafka.connect.transform.flatten
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue
import com.celonis.kafka.connect.transform.InferSchemaAndNormaliseValue.ValueAndSchema
import org.apache.kafka.connect.data.Schema

/**
  * Add inference and normalisation on top of an existing flattener
  */
private final class NormalisingFlattener(private[flatten] val innerFlattener: Flattener) extends Flattener {
  override def flatten(originalValue: Any, originalSchema: Option[Schema]): Any = {
    val valueAndSchema = originalSchema match {
      case Some(originalSchema) => ValueAndSchema(originalValue, originalSchema)
      case None => InferSchemaAndNormaliseValue(originalValue).getOrElse(ValueAndSchema(originalValue, Schema.BYTES_SCHEMA))
    }

    innerFlattener.flatten(valueAndSchema.normalisedValue, Some(valueAndSchema.schema))
  }
}
