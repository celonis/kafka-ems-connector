/*
 * Copyright 2024 Celonis SE
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
