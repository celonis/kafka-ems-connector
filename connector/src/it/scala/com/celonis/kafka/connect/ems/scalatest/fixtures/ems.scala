package com.celonis.kafka.connect.ems.scalatest.fixtures

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import com.celonis.kafka.connect.ems.sink.EmsSinkTask
import com.celonis.kafka.connect.ems.randomEmsTable
import com.celonis.kafka.connect.ems.randomTopicName

import scala.jdk.CollectionConverters.MapHasAsJava

object ems {

  def withEmsSinkTask(
    endpoint:          String,
    connectorName:     String         = "ems",
    commitRecords:     String         = "1",
    commitSize:        String         = "1000000",
    commitInterval:    String         = "3600000",
    obfuscationType:   Option[String] = None,
    obfuscationFields: Option[String] = None,
    sha512Salt:        Option[String] = None,
  )(testCode:          (String, EmsSinkTask, String) => Any,
  ): Unit = {
    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()
    var props = Map(
      "name"                                     -> connectorName,
      "connector.class"                          -> "com.celonis.kafka.connect.ems.sink.EmsSinkConnector",
      "tasks.max"                                -> "1",
      "topics"                                   -> sourceTopic,
      EmsSinkConfigConstants.ENDPOINT_KEY        -> endpoint,
      EmsSinkConfigConstants.AUTHORIZATION_KEY   -> "AppKey key",
      EmsSinkConfigConstants.TARGET_TABLE_KEY    -> emsTable,
      EmsSinkConfigConstants.COMMIT_RECORDS_KEY  -> commitRecords,
      EmsSinkConfigConstants.COMMIT_SIZE_KEY     -> commitSize,
      EmsSinkConfigConstants.COMMIT_INTERVAL_KEY -> commitInterval,
      EmsSinkConfigConstants.TMP_DIRECTORY_KEY   -> "/tmp/",
      EmsSinkConfigConstants.ERROR_POLICY_KEY    -> "CONTINUE",
    )
    sha512Salt.foreach(p => props += (EmsSinkConfigConstants.SHA512_SALT_KEY -> p))
    obfuscationType.foreach(p => props += (EmsSinkConfigConstants.OBFUSCATION_TYPE_KEY -> p))
    obfuscationFields.foreach(p => props += (EmsSinkConfigConstants.OBFUSCATED_FIELDS_KEY -> p))
    val task = new EmsSinkTask()
    try {
      task.start(props.asJava)
      val _ = testCode(connectorName, task, sourceTopic)
    } finally {
      task.stop()
    }
  }
}
