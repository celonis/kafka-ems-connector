/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import com.celonis.kafka.connect.transform.LeafNode
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.storage.ConverterType

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

object ArrayFlattener extends LazyLogging {

  private val jsonConverter = new JsonConverter()
  jsonConverter.configure(
    Map(
      "converter.type" -> ConverterType.KEY.getName,
      "schemas.enable" -> "false",
    ).asJava,
  )

  def flatten(
    path:             Seq[String],
    possiblyNullList: util.List[_],
    schema:           Option[Schema],
  )(
               implicit
               config: FlattenerConfig,
  ): Seq[LeafNode] =
    if (config.discardCollections)
      Nil
    else
      Try {
        Option(possiblyNullList).map {
          list =>
            val asJson = new String(jsonConverter.fromConnectData("anyTopic", schema.orNull, list))
            LeafNode(path, asJson)
        }.view.toSeq
      }.recover { error =>
        logger.error(s"Unable to flatten array type ${possiblyNullList.getClass.getName} with schema ${schema.orNull}",
                     error,
        )
        Seq.empty
      }.getOrElse(Seq.empty)
}
