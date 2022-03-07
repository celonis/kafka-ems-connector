/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.config.EmsSinkConfigDef
import com.celonis.kafka.connect.ems.utils.JarManifest
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.Logger

import java.util
import scala.jdk.CollectionConverters._
class EmsSinkConnector extends SinkConnector {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest.from(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var props: util.Map[String, String] = _

  override def version(): String = manifest.version.getOrElse("Unknown")

  override def taskClass(): Class[_ <: Task] = classOf[EmsSinkTask]

  override def config(): ConfigDef = EmsSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Creating EMS sink connector")
    this.props = props
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.debug(s"Creating $maxTasks tasks config")
    List.fill(maxTasks)(props).asJava
  }
}
