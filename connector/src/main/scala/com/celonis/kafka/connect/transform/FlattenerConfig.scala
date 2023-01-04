package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

case class FlattenerConfig(
  discardCollections: Boolean                                = false,
  jsonBlobChunks:     Option[FlattenerConfig.JsonBlobChunks] = None,
)

object FlattenerConfig {
  case class JsonBlobChunks(chunkFields: Int, emsVarcharLength: Int)
  case object FallbackVarcharLengthRequired extends Throwable {
    private val prefix = "transform.flatten"
    override val getMessage =
      s"$prefix.${JsonBlobMaxChunks} supplied without a $prefix.$FallbackVarcharLength value. Please try supplying a value for both these keys."
  }

  final val DiscardCollections    = "collections.discard"
  final val JsonBlobMaxChunks     = "jsonblob.chunks.max"
  final val FallbackVarcharLength = "fallback.varchar.length"

  def configDef = new ConfigDef()
    .define(
      DiscardCollections,
      Type.BOOLEAN,
      false,
      Importance.MEDIUM,
      "Discard array and map fields at any level of depth",
    )
    .define(
      JsonBlobMaxChunks,
      Type.INT,
      null,
      Importance.MEDIUM,
      "Encodes the record into a JSON blob broken down into N VARCHAR fields (e.g. `payload_chunk1`, `payload_chunk2`, `...`, `payload_chunkN`).",
    )
    //TODO: add validator
    .define(
      FallbackVarcharLength,
      Type.INT,
      null,
      Importance.HIGH,
      EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_DOC,
    )

  def apply(confMap: java.util.Map[String, _]): FlattenerConfig = {
    val abstractConfig     = new AbstractConfig(configDef, confMap)
    val discardCollections = abstractConfig.getBoolean(DiscardCollections)
    val jsonBlobMaxChunks  = Option(abstractConfig.getInt(JsonBlobMaxChunks)).map(_.toInt)
    val jsonBlobConfig = jsonBlobMaxChunks.map { maxChunks =>
      Option(abstractConfig.getInt(FallbackVarcharLength)).map { varcharLength =>
        JsonBlobChunks(maxChunks, varcharLength.toInt)
      }.getOrElse(
        throw FallbackVarcharLengthRequired,
      )

    }

    FlattenerConfig(
      discardCollections,
      jsonBlobConfig,
    )
  }
}
