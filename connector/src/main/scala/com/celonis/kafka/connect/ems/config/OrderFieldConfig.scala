/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.syntax.option._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.ORDER_FIELD_NAME_KEY
import com.celonis.kafka.connect.ems.conversion.NoOpOrderFieldInserter
import com.celonis.kafka.connect.ems.conversion.OrderFieldInserter

case class OrderFieldConfig(name: Option[String], inserter: OrderFieldInserter)

object OrderFieldConfig {
  def from(props: Map[String, _], primaryKeys: List[String]): OrderFieldConfig = {
    val configFieldName = PropertiesHelper.getString(props, ORDER_FIELD_NAME_KEY).map(_.trim).filter(_.nonEmpty)
    val name =
      if (primaryKeys.nonEmpty)
        configFieldName.orElse(
          OrderFieldInserter.FieldName.some,
        )
      else None

    val inserter = if (primaryKeys.nonEmpty) {
      // if a value is set by the user, then we don't use the auto-injected one. It is expected the field name is
      // present since the connector won't validate it
      configFieldName.map(_ => NoOpOrderFieldInserter).getOrElse(OrderFieldInserter)
    } else {
      // we don't have a PK, therefore do not use any inserter
      NoOpOrderFieldInserter
    }
    OrderFieldConfig(name, inserter)
  }
}
