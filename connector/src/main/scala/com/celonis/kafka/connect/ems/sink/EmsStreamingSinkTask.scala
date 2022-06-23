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

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.celonis.kafka.connect.ems.config.EmsStreamingSinkConfig
import com.celonis.kafka.connect.ems.config.EmsStreamingSinkConfigDef
import com.celonis.kafka.connect.ems.conversion.DataplaneConverter
import com.celonis.kafka.connect.ems.dataplane.DataplaneClient
import com.celonis.kafka.connect.ems.dataplane.IngressInputAvro
import com.celonis.kafka.connect.ems.dataplane.IngressInputMessageAvro
import com.celonis.kafka.connect.ems.errors.ErrorPolicy.Retry
import com.celonis.kafka.connect.ems.utils.JarManifest
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import org.apache.avro.Schema
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters._

class EmsStreamingSinkTask extends SinkTask with StrictLogging {
  import EmsStreamingSinkTask._

  private var state: Option[State] = None

  override def start(props: util.Map[String, String]): Unit = {
    import cats.effect.unsafe.implicits._
    startIO(props).unsafeRunSync()
  }

  override def stop(): Unit = {
    import cats.effect.unsafe.implicits._
    stopIO.unsafeRunSync()
  }

  private def startIO(props: util.Map[String, String]): IO[Unit] =
    taskResources(props).allocated.flatMap {
      case (resources, finalizer) =>
        IO(this.state = Some(State(resources, finalizer)))
    }.void

  private def taskResources(props: util.Map[String, String]): Resource[IO, EmsStreamingSinkTask.Resources] = for {
    validProps <- IO(EmsStreamingSinkConfigDef.taskConfig.parse(props).asScala.toMap).toResource
    config     <- EmsStreamingSinkConfig.ForTask.from(validProps).leftMap(new ConfigException(_)).liftTo[Resource[IO, *]]
    _          <- maybeSetErrorInterval(config.baseConfig).toResource
    client     <- DataplaneClient.resource[IO](config.baseConfig.authorization, config.baseConfig.endpoint)
    converter <-
      DataplaneConverter.toAvroMessage[IO](config.baseConfig.keySchema, config.baseConfig.valueSchema).toResource
    keySchema   <- config.baseConfig.keySchema.traverse(schemaToJson).toResource
    valueSchema <- schemaToJson(config.baseConfig.valueSchema).toResource
    dispatcher  <- Dispatcher[IO]
  } yield EmsStreamingSinkTask.Resources(config, client, converter, keySchema, valueSchema, dispatcher)

  private def schemaToJson(schema: Schema): IO[Json] =
    IO.fromEither(io.circe.parser.parse(schema.toString))

  // blindly copied from existing connector
  private def maybeSetErrorInterval(config: EmsStreamingSinkConfig): IO[Unit] = IO {
    //if error policy is retry set retry interval
    config.errorPolicy match {
      case Retry => Option(context).foreach(_.timeout(config.retries.interval))
      case _     =>
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit =
    execute {
      case Resources(config, client, converter, keySchema, valueSchema, _) =>
        val recordsVector = records.asScala.toVector
        recordsVector.lastOption match {
          case Some(last) =>
            recordsVector
              .traverse(converter.convert)
              .flatMap { messages =>
                client.writeMessagesAvro(config.targetTopic, IngressInputAvro(keySchema, valueSchema, messages.toList))
                  .flatTap(_ => IO(logger.info(s"Wrote ${recordsVector.size} msgs up to offset ${last.kafkaOffset()}")))

              }
          case None =>
            IO.unit
        }
    }

  private def execute(f: Resources => IO[Unit]): Unit = state match {
    case Some(state) => state.resources.dispatcher.unsafeRunSync(f(state.resources))
    case None        => throw new IllegalStateException("Task not initialized")
  }

  private def stopIO: IO[Unit] = (state.traverse(_.finalizer) *> IO(this.state = None)).void

  override def version(): String =
    JarManifest.from(getClass.getProtectionDomain.getCodeSource.getLocation)
      .version.getOrElse("unknown")
}

object EmsStreamingSinkTask {
  final case class Resources(
    config:      EmsStreamingSinkConfig.ForTask,
    client:      DataplaneClient[IO],
    converter:   DataplaneConverter[IO, IngressInputMessageAvro],
    keySchema:   Option[Json],
    valueSchema: Json,
    dispatcher:  Dispatcher[IO],
  )
  final case class State(resources: Resources, finalizer: IO[Unit])
}
