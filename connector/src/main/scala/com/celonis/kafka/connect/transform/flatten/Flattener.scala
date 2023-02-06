package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import org.apache.kafka.connect.data.Schema

trait Flattener {
  def flatten(value: Any, schema: Schema): Any
}

object Flattener {
  val noOpFlattener: Flattener = new Flattener {
    override def flatten(value: Any, schema: Schema): Any = value
  }

  def fromConfig(config: Option[FlattenerConfig]): Flattener =
    config match {
      case Some(config) => config.jsonBlobChunks match {
          case Some(jsonBlobChunks) => new ChunkedJsonBlobFlattener(jsonBlobChunks)
          case None                 => new StructFlattener(new SchemaFlattener(config.discardCollections))
        }
      case None => noOpFlattener
    }
}
