/*
 * Copyright 2017-2022 Celonis Ltd
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
