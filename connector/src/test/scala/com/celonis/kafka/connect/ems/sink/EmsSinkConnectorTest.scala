package com.celonis.kafka.connect.ems.sink

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava}

class EmsSinkConnectorTest extends AnyFunSuite with Matchers {

  test("starts an EMS sink connector") {
    val props = Map(
      EmsSinkConfigConstants.ENDPOINT_KEY        -> "https://mockserver.celonis.cloud:1024",
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> "target-table",
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "1",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000000",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val connector = new EmsSinkConnector()
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.ENDPOINT_KEY) shouldBe "https://mockserver.celonis.cloud:1024"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.AUTHORIZATION_KEY) shouldBe "AppKey key"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.TARGET_TABLE_KEY) shouldBe "target-table"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.COMMIT_RECORDS_KEY) shouldBe "1"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.COMMIT_SIZE_KEY) shouldBe "1000000"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.COMMIT_INTERVAL_KEY) shouldBe "3600000"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.TMP_DIRECTORY_KEY) shouldBe "/tmp/"
    taskConfigs.asScala.head.get(EmsSinkConfigConstants.ERROR_POLICY_KEY) shouldBe "CONTINUE"
    taskConfigs.size() shouldBe 1
    connector.taskClass() shouldBe classOf[EmsSinkTask]
    connector.version() shouldBe "Unknown"
    connector.stop()
  }

  test("throws an exception for invalid config value") {
    val props = Map(
      EmsSinkConfigConstants.ENDPOINT_KEY        -> "https://mockserver.celonis.cloud:1024",
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> "target-table",
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> "1",
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> "1000000L",
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> "3600000",
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    ).asJava

    val connector = new EmsSinkConnector()

    val thrown = the[ConfigException] thrownBy connector.config().parse(props)
    thrown.getMessage should startWith("Invalid value")
  }

  test("throws an exception for missing required connector config") {
    val props = Map(
      EmsSinkConfigConstants.ENDPOINT_KEY -> "https://mockserver.celonis.cloud:1024",
    ).asJava

    val connector = new EmsSinkConnector()

    val thrown = the[ConfigException] thrownBy connector.config().parse(props)
    thrown.getMessage should include regex("^.*Missing required configuration.*$")
  }
}
