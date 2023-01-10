package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.FLATTENER_JSONBLOB_CHUNKS_KEY
import com.celonis.kafka.connect.transform.FlattenerConfig.JsonBlobChunks
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.storage.ConverterType

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

object ChunkedJsonBlob {
  case class MisconfiguredJsonBlobMaxChunks(configuredChunksSize: Int, blobByteSize: Int, emsVarcharLength: Int)
      extends Throwable {
    override def getMessage: String =
      s"Configured value ${configuredChunksSize} for ${FLATTENER_JSONBLOB_CHUNKS_KEY} is insufficient! Current JSON blob length: $blobByteSize, Ems VARCHAR Length: ${emsVarcharLength}."
  }

  private[flatten] def schema(config: JsonBlobChunks): Schema =
    (1 to config.chunks).foldLeft(SchemaBuilder.struct()) { (builder, idx) =>
      builder.field(s"payload_chunk$idx", Schema.OPTIONAL_STRING_SCHEMA)
    }.schema()

  def asConnectData(value: AnyRef)(implicit config: JsonBlobChunks): Struct = {
    val FlattenerConfig.JsonBlobChunks(maxChunks, emsVarcharLength) = config

    val jsonBlobBytes = value match {
      case struct: Struct => jsonConverter.fromConnectData("some-topic", struct.schema(), struct)
      case str:    String => str.getBytes(StandardCharsets.UTF_8)
      case _ => jacksonMapper.writeValueAsString(value).getBytes(StandardCharsets.UTF_8)
    }
    val numChunks = jsonBlobBytes.length / emsVarcharLength
    if (numChunks > maxChunks)
      throw MisconfiguredJsonBlobMaxChunks(maxChunks, jsonBlobBytes.length, emsVarcharLength)
    else
      jsonBlobBytes.grouped(emsVarcharLength).zipWithIndex.foldLeft(new Struct(schema(config))) {
        case (newStruct, (jsonBlobChunk, idx)) =>
          newStruct.put(s"payload_chunk${idx + 1}", new String(jsonBlobChunk, StandardCharsets.UTF_8))
      }
  }

  private val jacksonMapper = new ObjectMapper()

  private[flatten] lazy val jsonConverter = {
    val converter = new JsonConverter()
    converter.configure(
      Map(
        "converter.type" -> ConverterType.VALUE.getName,
        "schemas.enable" -> "false",
      ).asJava,
    )
    converter

  }

}
