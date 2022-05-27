/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.sink
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_RECORDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.COMMIT_SIZE_KEY
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.Collections
import scala.jdk.CollectionConverters.MapHasAsJava

class EmsSinkConfiguratorTest extends AnyFunSuite with Matchers {

  val emsSinkConfigurator = new DefaultEmsSinkConfigurator

  test("should extract name from pros") {
    val props = Map(
      "name" -> "ems",
    ).asJava

    emsSinkConfigurator.getSinkName(props) shouldBe "ems"
  }

  test("should return default name if none configured") {
    emsSinkConfigurator.getSinkName(Collections.emptyMap()) shouldBe "MissingSinkName"
  }

  test(s"throws exception when $COMMIT_SIZE_KEY less than 1MB") {
    val props = Map(
      "name"                                     -> "ems",
      EmsSinkConfigConstants.ENDPOINT_KEY        -> "https://celonis.cloud",
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> "target-table",
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "1",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val thrown = the[ConnectException] thrownBy emsSinkConfigurator.getEmsSinkConfig(props)
    thrown.getMessage should include regex "^.*Flush size needs to be at least 1000000.*$"
  }

  test(s"throws exception when $COMMIT_RECORDS_KEY is negative") {
    val props = Map(
      "name"                                     -> "ems",
      EmsSinkConfigConstants.ENDPOINT_KEY        -> "https://celonis.cloud",
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> "target-table",
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "-1",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000000",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val thrown = the[ConnectException] thrownBy emsSinkConfigurator.getEmsSinkConfig(props)
    thrown.getMessage should include regex "^.*Uploading the data to EMS requires a record count greater than 0.*$"
  }
}
