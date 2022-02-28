/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.model

import cats.Show
import cats.implicits.toShow
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }

class Topic(val value: String) extends AnyVal
object Topic {
  implicit val show: Show[Topic] = Show.show(_.value)
}

class Partition(val value: Int) extends AnyVal
object Partition {
  implicit val show: Show[Partition] = Show.show(_.value.toString)
}
class Offset(val value: Long) extends AnyVal {
  def >(other: Offset): Boolean = value > other.value
}

object Offset {
  implicit def orderingByOffsetValue[A <: Offset]: Ordering[A] =
    Ordering.by(_.value)

  implicit val show: Show[Offset] = Show.show(_.value.toString)

}

case class TopicPartition(topic: Topic, partition: Partition) {
  def withOffset(offset: Offset): TopicPartitionOffset = TopicPartitionOffset(topic, partition, offset)

  def toKafka: KafkaTopicPartition = new KafkaTopicPartition(topic.value, partition.value)
}

object TopicPartition {
  def from(kafkaTopicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(new Topic(kafkaTopicPartition.topic()), new Partition(kafkaTopicPartition.partition()))

  implicit val show: Show[TopicPartition] = Show.show(tp => s"Topic=${tp.topic.show} Partition=${tp.partition.show}")
}

case class TopicPartitionOffset(topic: Topic, partition: Partition, offset: Offset) {
  def toTopicPartition: TopicPartition = TopicPartition(topic, partition)
}
