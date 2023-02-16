package com.celonis.kafka.connect.transform.fields

final case class EmbeddedKafkaMetadata(partition: Int, offset: Long, timestamp: Long) {
  def partitionOffset = s"${partition}_${offset}"
}
