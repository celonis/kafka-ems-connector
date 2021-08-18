/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import cats.Show
import cats.implicits.toShow
import org.apache.kafka.connect.data.Struct

case class RecordMetadata(topicPartition: TopicPartition, offset: Offset)
object RecordMetadata {
  implicit val show: Show[RecordMetadata] = Show.show { m =>
    m.topicPartition.topic.show + "-" + m.topicPartition.partition.show + ":" + m.offset.show
  }
}
case class Record(value: Struct, metadata: RecordMetadata)
