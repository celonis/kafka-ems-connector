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

package com.celonis.kafka.connect.ems.utils

import cats.Show
import com.celonis.kafka.connect.ems.utils

import java.io.File
import java.net.URL
import java.util.jar.JarFile
import scala.jdk.CollectionConverters._

class JarManifest(private val map: Map[String, String]) extends AnyVal {
  def version: Option[String] = map.get("version")
}

object JarManifest {
  implicit val show: Show[JarManifest] = Show.show { jarManifest =>
    jarManifest.map.map { case (k, v) => s"$k\t\t$v" }.mkString(System.lineSeparator())
  }
  def from(location: URL): JarManifest = {
    val builder = Map.newBuilder[String, String]
    val file    = new File(location.toURI)
    if (file.isFile) {
      val jarFile    = new JarFile(file)
      val manifest   = jarFile.getManifest
      val attributes = manifest.getMainAttributes
      attributes.keySet()
        .asScala
        .collect { case s: String => s }
        .foreach { key =>
          builder += (key -> attributes.getValue(key))
        }
    }
    new utils.JarManifest(builder.result())
  }
}
