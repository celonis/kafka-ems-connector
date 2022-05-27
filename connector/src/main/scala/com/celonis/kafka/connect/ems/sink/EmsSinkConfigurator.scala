package com.celonis.kafka.connect.ems.sink
import cats.implicits._
import com.celonis.kafka.connect.ems.config.EmsSinkConfig
import com.celonis.kafka.connect.ems.config.EmsSinkConfigDef
import org.apache.kafka.connect.errors.ConnectException

import java.util
import scala.jdk.CollectionConverters._
import scala.util.Try

trait EmsSinkConfigurator {

  def getSinkName(props: util.Map[String, String]): String

  def getEmsSinkConfig(props: util.Map[String, String]): EmsSinkConfig
}

class DefaultEmsSinkConfigurator extends EmsSinkConfigurator {

  def getSinkName(props: util.Map[String, String]): String =
    Option(props.get("name")).filter(_.trim.nonEmpty).getOrElse("MissingSinkName")

  def getEmsSinkConfig(props: util.Map[String, String]): EmsSinkConfig = {
    val config: EmsSinkConfig = {
      for {
        parsedConfigDef <- Try(EmsSinkConfigDef.config.parse(props).asScala.toMap).toEither.leftMap(_.getMessage)
        config          <- EmsSinkConfig.from(getSinkName(props), parsedConfigDef)
      } yield config
    }.leftMap(err => throw new ConnectException(err)).merge
    config
  }
}
