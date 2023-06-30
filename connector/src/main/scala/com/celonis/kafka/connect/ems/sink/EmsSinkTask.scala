/*
 * Copyright 2023 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.celonis.kafka.connect.ems.sink

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.errors.ErrorPolicy
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.model._
import com.celonis.kafka.connect.ems.storage.EmsUploader
import com.celonis.kafka.connect.ems.storage.FileSystemOperations
import com.celonis.kafka.connect.ems.storage.Writer
import com.celonis.kafka.connect.ems.storage.WriterManager
import com.celonis.kafka.connect.ems.utils.Version
import com.celonis.kafka.connect.transform.RecordTransformer
import com.typesafe.scalalogging.StrictLogging
import okhttp3.OkHttpClient
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters._

class EmsSinkTask extends SinkTask with StrictLogging {

  private var writerManager: WriterManager[IO] = _
  private var sinkName:      String            = _

  private var maxRetries:          Int                 = 0
  private var retriesLeft:         Int                 = maxRetries
  private var errorPolicy:         ErrorPolicy         = ErrorPolicy.Retry
  private var transformer:         RecordTransformer   = _
  private val emsSinkConfigurator: EmsSinkConfigurator = new DefaultEmsSinkConfigurator
  private var okHttpClient:        OkHttpClient        = _

  override def version(): String = Version.implementationVersion

  override def start(props: util.Map[String, String]): Unit = {
    sinkName = emsSinkConfigurator.getSinkName(props)

    logger.info(
      s"[{}] EmsSinkTask.start {}. Git head commit: {}",
      sinkName,
      Version.implementationVersion,
      com.celonis.kafka.connect.BuildInfo.gitHeadCommit,
    )
    val config: EmsSinkConfig = emsSinkConfigurator.getEmsSinkConfig(props)
    okHttpClient = config.http.createHttpClient()

    maybeSetErrorInterval(config)

    val writers = Ref.unsafe[IO, Map[TopicPartition, Writer]](Map.empty)

    writerManager = {
      val fileSystemOperations =
        if (config.useInMemoryFileSystem)
          FileSystemOperations.InMemory
        else
          FileSystemOperations.Default

      WriterManager.from[IO](
        config,
        sinkName,
        new EmsUploader[IO](
          config.url,
          config.authorization.header,
          config.target,
          config.connectionId,
          s"Kafka2Ems $version",
          config.fallbackVarCharLengths,
          if (config.primaryKeys.exists(_.trim.nonEmpty))
            Some(NonEmptyList.fromListUnsafe(config.primaryKeys.map(_.trim).filter(_.nonEmpty)))
          else None,
          config.http,
          config.orderField.name,
          okHttpClient,
        ),
        writers,
        fileSystemOperations,
      )
    }

    maxRetries  = config.retries.retries
    retriesLeft = maxRetries
    errorPolicy = config.errorPolicy
    transformer = RecordTransformer.fromConfig(config)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    val io = for {
      _ <- IO(logger.info("[{}] EmsSinkTask:put records={}", sinkName, records.size()))
      _ <- records.asScala
        // filter our "deletes" for now
        .filter(_.value() != null)
        .toList
        .traverse { record =>
          for {
            avroRecord <- transformer.transform(record)
            tp          = TopicPartition(new Topic(record.topic()), new Partition(record.kafkaPartition()))
            metadata    = RecordMetadata(tp, new Offset(record.kafkaOffset()))
            _          <- writerManager.write(Record(avroRecord, metadata))
          } yield ()
        }
      _ <- if (records.isEmpty) writerManager.maybeUploadData
      else IO(())
    } yield ()

    io.attempt.unsafeRunSync() match {
      case Left(value) =>
        retriesLeft -= 1
        errorPolicy.handle(value, retriesLeft)
      case Right(_) =>
        retriesLeft = maxRetries
    }
  }

  override def preCommit(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata]): util.Map[KafkaTopicPartition,
                                                                                OffsetAndMetadata,
  ] = {
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

    (for {
      offsets <- writerManager
        .preCommit(topicPartitionOffsetTransformed)
        .map { map =>
          map.map {
            case (topicPartition, offsetAndMetadata) =>
              (topicPartition.toKafka, offsetAndMetadata)
          }.asJava
        }
      _ <- IO(logger.debug(s"[{}] Returning latest written offsets={}", sinkName: Any, getDebugInfo(offsets): Any))
    } yield offsets).unsafeRunSync()
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val partitionsDebug = partitions.asScala.map(tp => s"${tp.topic()}-${tp.partition()}").mkString(",")
    logger.debug(s"[{}] Open partitions", sinkName, partitionsDebug)
    val topicPartitions = partitions.asScala
      .map(tp => TopicPartition(new Topic(tp.topic), new Partition(tp.partition)))
      .toSet

    writerManager
      .open(topicPartitions)
      .unsafeRunSync()
  }

  /** Whenever close is called, the topics and partitions assigned to this task may be changing, eg, in a re-balance.
    * Therefore, we must commit our open files for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val topicPartitions =
      partitions.asScala.map(tp => TopicPartition(new Topic(tp.topic()), new Partition(tp.partition()))).toList
    (for {
      _ <- IO(logger.debug(s"[{}] EmsSinkTask.close with {} partitions", sinkName, partitions.size()))
      _ <- Option(writerManager).fold(IO(())) {
        _.close(topicPartitions)
      }
    } yield ()).attempt.unsafeRunSync() match {
      case Left(value) =>
        logger.warn(
          s"[$sinkName]There was an error closing the partitions: ${topicPartitions.map(_.show).mkString(",")}]]",
          value,
        )
      case Right(_) =>
    }
  }

  override def stop(): Unit = {
    (for {
      _ <- IO(logger.debug(s"[{}] EmsSinkTask.Stop", sinkName))
      _ <- Option(writerManager).fold(IO(()))(_.close)
      _ <- Option(okHttpClient).fold(IO(()))(c => IO(c.connectionPool().evictAll()))
    } yield ()).attempt.unsafeRunSync() match {
      case Left(value) =>
        logger.warn(s"[$sinkName]There was an error stopping the EmsSinkTask", value)
      case Right(_) =>
    }
    writerManager = null
  }

  private def maybeSetErrorInterval(config: EmsSinkConfig): Unit =
    // if error policy is retry set retry interval
    config.errorPolicy match {
      case Retry => Option(context).foreach(_.timeout(config.retries.interval))
      case _     =>
    }
}
