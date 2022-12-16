package com.celonis.kafka.connect.transform.flatten

import org.apache.kafka.connect.data.{ Date => KafkaDate }
import org.apache.kafka.connect.data.{ Decimal => KafkaDecimal }
import org.apache.kafka.connect.data.{ Time => KafkaTime }
import org.apache.kafka.connect.data.{ Timestamp => KafkaTimestamp }

object LogicalTypes {

  val logicalTypeSchemas = Map(
    KafkaDecimal.LOGICAL_NAME   -> KafkaDecimal.builder(0).optional().build(), // TODO
    KafkaDate.LOGICAL_NAME      -> KafkaDate.builder().optional().build(),
    KafkaTime.LOGICAL_NAME      -> KafkaTime.builder().optional().build(),
    KafkaTimestamp.LOGICAL_NAME -> KafkaTimestamp.builder().optional().build(),
  )

}
