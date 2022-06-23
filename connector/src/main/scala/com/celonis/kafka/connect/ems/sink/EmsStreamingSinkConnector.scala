/*
 * Copyright 2022 Celonis SE
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
import cats.syntax.all._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import com.celonis.kafka.connect.ems.config.EmsStreamingSinkConfig
import com.celonis.kafka.connect.ems.config.EmsStreamingSinkConfigDef
import com.celonis.kafka.connect.ems.dataplane.DataplaneClient
import com.celonis.kafka.connect.ems.dataplane.IngestionSetupRequest
import com.celonis.kafka.connect.ems.utils.JarManifest
import org.apache.avro.Schema
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.Logger

import java.util
import scala.jdk.CollectionConverters._

class EmsStreamingSinkConnector extends SinkConnector {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private var taskProps: util.Map[String, String] = _

  override def version(): String =
    JarManifest.from(getClass.getProtectionDomain.getCodeSource.getLocation).version.getOrElse("Unknown")

  override def taskClass(): Class[_ <: Task] = classOf[EmsStreamingSinkTask]

  override def config(): ConfigDef = EmsStreamingSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    import cats.effect.unsafe.implicits.global
    startIO(props).unsafeRunSync()
  }

  private def startIO(props: util.Map[String, String]): IO[Unit] = {
    def schemaToJson(s: Schema) = io.circe.parser.parse(s.toString).liftTo[IO]

    for {
      _ <- IO(logger.info(s"Creating EMS streaming sink connector"))

      validProps <- IO(EmsStreamingSinkConfigDef.config.parse(props).asScala.toMap)
      config     <- EmsStreamingSinkConfig.from(validProps).leftMap(new ConfigException(_)).liftTo[IO]

      maybeKeySchema <- config.keySchema.traverse(schemaToJson)
      valueSchema    <- schemaToJson(config.valueSchema)

      request = IngestionSetupRequest(
        dataPoolId  = config.dataPoolId,
        tableName   = config.target,
        keySchema   = maybeKeySchema,
        valueSchema = valueSchema,
        partitions  = None,
        replication = Some(2), // to match cel-1 min.insync.replicas default of 2
      )
      resp <- DataplaneClient.resource[IO](config.authorization, config.endpoint).use(
        _.setup(NonEmptyList.one(request)),
      )

      targetTopic <- resp.topicNames.get(config.target)
        .liftTo[IO](new ConnectException("Internal error: no topic name returned"))

      _ = println(s"Got target topic: $targetTopic")

      _ <- IO(this.taskProps = props.asScala.toMap.updated(EmsSinkConfigConstants.TARGET_TOPIC_KEY, targetTopic).asJava)

    } yield ()

  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.debug(s"Creating $maxTasks tasks config")
    println(s"Outputting $maxTasks task configs of ${taskProps.asScala}")
    List.fill(maxTasks)(taskProps).asJava
  }
}
