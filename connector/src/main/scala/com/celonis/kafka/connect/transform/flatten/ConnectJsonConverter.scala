package com.celonis.kafka.connect.transform.flatten

import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.storage.ConverterType
import scala.util.chaining._
import scala.jdk.CollectionConverters._

private object ConnectJsonConverter {
  lazy val converter: JsonConverter =
    new JsonConverter().tap(
      _.configure(
        Map(
          "converter.type" -> ConverterType.VALUE.getName,
          "schemas.enable" -> "false",
        ).asJava,
      ),
    )
}
