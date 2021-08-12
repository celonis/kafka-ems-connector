/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.config.EmsSinkConfigDef
import com.celonis.kafka.connect.ems.conversion.ValueToSinkDataConverter
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.model.Offset
import com.celonis.kafka.connect.ems.model.Partition
import com.celonis.kafka.connect.ems.model.Record
import com.celonis.kafka.connect.ems.model.RecordMetadata
import com.celonis.kafka.connect.ems.model.SinkData
import com.celonis.kafka.connect.ems.model.Topic
import com.celonis.kafka.connect.ems.model.TopicPartition
import com.celonis.kafka.connect.ems.storage.EmsUploader
import com.celonis.kafka.connect.ems.utils.JarManifest
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class EmsSinkTask extends SinkTask with StrictLogging {

  private var writerManager: WriterManager = _

  private var sinkName: String = _

  override def version(): String =
    JarManifest.from(getClass.getProtectionDomain.getCodeSource.getLocation)
      .version.getOrElse("unknown")

  override def start(props: util.Map[String, String]): Unit = {
    sinkName = getSinkName(props).getOrElse("MissingSinkName")

    logger.debug(s"[{}] EmsSinkTask.start", sinkName)

    val config = EmsSinkConfig.from(sinkName, EmsSinkConfigDef.config.parse(props).asScala.toMap) match {
      case Left(value)  => throw new ConnectException(value)
      case Right(value) => value
    }
    maybeSetErrorInterval(config)

    writerManager = WriterManager.from(config, sinkName, new EmsUploader(config.url, config.authorizationKey))
  }

  private def getSinkName(props: util.Map[String, String]): Option[String] =
    Option(props.get("name")).filter(_.trim.nonEmpty)

  /* private def propsFromContext(props: util.Map[String, String]): util.Map[String, String] =
    Option(context)
      .flatMap(c => Option(c.configs()))
      .filter(_.isEmpty == false)
      .getOrElse(props)*/

  private def maybeSetErrorInterval(config: EmsSinkConfig): Unit =
    //if error policy is retry set retry interval
    config.errorPolicy match {
      case Retry => context.timeout(config.retries.interval)
      case _     =>
    }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    logger.debug("[{}] EmsSinkTask:put records={}", sinkName, records.size())

    records.asScala
      //filter our "deletes
      .filter(_.value() != null)
      .foreach {
        record =>
          writerManager.write(
            Record(
              Option(record.key()).fold(Option.empty[SinkData])(key =>
                Option(ValueToSinkDataConverter(key, Option(record.keySchema()))),
              ),
              ValueToSinkDataConverter(record.value(), Option(record.valueSchema())),
              RecordMetadata(TopicPartition(new Topic(record.topic()), new Partition(record.kafkaPartition())),
                             new Offset(record.kafkaOffset()),
              ),
            ),
          )
      }

    if (records.isEmpty) writerManager.maybeUploadData()
    ()
  }

  override def preCommit(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata],
  ): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {
    def getDebugInfo(in: util.Map[KafkaTopicPartition, OffsetAndMetadata]): String =
      in.asScala.map {
        case (k, v) =>
          k.topic() + "-" + k.partition() + "=" + v.offset()
      }.mkString(";")

    logger.debug(s"[{}] preCommit with offsets={}",
                 sinkName,
                 getDebugInfo(Option(currentOffsets).getOrElse(new util.HashMap())): Any,
    )

    val topicPartitionOffsetTransformed: Map[TopicPartition, OffsetAndMetadata] =
      Option(currentOffsets)
        .getOrElse(new util.HashMap())
        .asScala
        .map {
          case (tp, offsetAndMetadata) =>
            TopicPartition(new Topic(tp.topic()), new Partition(tp.partition())) -> offsetAndMetadata
        }
        .toMap

    val actualOffsets = writerManager
      .preCommit(topicPartitionOffsetTransformed)
      .map {
        case (topicPartition, offsetAndMetadata) =>
          (topicPartition.toKafka, offsetAndMetadata)
      }.asJava

    logger.debug(s"[{}] Returning latest written offsets={}", sinkName: Any, getDebugInfo(actualOffsets): Any)
    actualOffsets
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val partitionsDebug = partitions.asScala.map(tp => s"${tp.topic()}-${tp.partition()}").mkString(",")
    logger.debug(s"[{}] Open partitions", sinkName, partitionsDebug)

    try {
      val topicPartitions = partitions.asScala
        .map(tp => TopicPartition(new Topic(tp.topic), new Partition(tp.partition)))
        .toSet

      writerManager
        .open(topicPartitions)
    } catch {
      case NonFatal(e) =>
        logger.error(s"[$sinkName] Error opening s3 sink writer", e)
        throw e
    }
  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a re-balance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(s"[{}] EmsSinkTask.close with {} partitions", sinkName, partitions.size())

    writerManager.close()
  }

  override def stop(): Unit = {
    logger.debug(s"[{}] EmsSinkTask.Stop", sinkName)

    writerManager.close()
    writerManager = null
  }

}
