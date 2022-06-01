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

package com.celonis.kafka.connect.ems.config

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class PropertiesHelperTest extends AnyFunSuite with Matchers {
  test("null and empty props mapped as empty") {
    val defs = new ConfigDef()

    defs.define("config_prop", Type.STRING, Importance.LOW, "")

    val mapWithNullProp = defs.parse(Map("config_prop" -> null).asJava)
    mapWithNullProp.get("config_prop") should be(null)
    PropertiesHelper.getString(mapWithNullProp.asScala.toMap, "config_prop") should be(None)

    val mapWithEmptyProp = defs.parse(Map("config_prop" -> "").asJava)
    mapWithEmptyProp.get("config_prop") should be("")
    PropertiesHelper.getString(mapWithEmptyProp.asScala.toMap, "config_prop") should be(None)
  }

}
