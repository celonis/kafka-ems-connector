/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }

class Topic(val value: String) extends AnyVal

class Partition(val value: Int) extends AnyVal

class Offset(val value: Long) extends AnyVal

object Offset {
  implicit def orderingByOffsetValue[A <: Offset]: Ordering[A] =
    Ordering.by(_.value)
}

case class TopicPartition(topic: Topic, partition: Partition) {
  def withOffset(offset: Offset): TopicPartitionOffset = TopicPartitionOffset(topic, partition, offset)

  def toKafka: KafkaTopicPartition = new KafkaTopicPartition(topic.value, partition.value)
}

object TopicPartition {
  def from(kafkaTopicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(new Topic(kafkaTopicPartition.topic()), new Partition(kafkaTopicPartition.partition()))
}

case class TopicPartitionOffset(topic: Topic, partition: Partition, offset: Offset) {
  def toTopicPartition: TopicPartition = TopicPartition(topic, partition)
}
