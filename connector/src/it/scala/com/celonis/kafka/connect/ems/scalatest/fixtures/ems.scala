package com.celonis.kafka.connect.ems.scalatest.fixtures

import cats.effect.IO
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants
import com.celonis.kafka.connect.ems.sink.EmsSinkTask
import com.celonis.kafka.connect.ems.randomEmsTable
import com.celonis.kafka.connect.ems.randomTopicName
import com.celonis.kafka.connect.transform.FlattenerConfig
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.MapHasAsJava

object ems {

  def withEmsSinkTask(
    endpoint:          String,
    connectorName:     String                  = "ems",
    commitRecords:     String                  = "1",
    commitSize:        String                  = "1000000",
    commitInterval:    String                  = "3600000",
    obfuscationType:   Option[String]          = None,
    obfuscationFields: Option[String]          = None,
    sha512Salt:        Option[String]          = None,
    flattenerConfig:   Option[FlattenerConfig] = None,
    sinkPutTimeout:    Option[String]          = None,
    tapRecord:         SinkRecord => IO[Unit]  = Function.const(IO.unit),
  )(testCode: (String, EmsSinkTask, String) => Any): Unit = {
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
    flattenerConfig.foreach { conf =>
      props += EmsSinkConfigConstants.FLATTENER_ENABLE_KEY -> "true"
      conf.jsonBlobChunks.foreach { jsonChunksConf =>
        props += EmsSinkConfigConstants.FLATTENER_JSONBLOB_CHUNKS_KEY -> jsonChunksConf.chunks.toString
        props += EmsSinkConfigConstants.FALLBACK_VARCHAR_LENGTH_KEY   -> jsonChunksConf.fallbackVarcharLength.toString
      }
    }
    sinkPutTimeout.foreach { timeoutInMillis =>
      props += EmsSinkConfigConstants.SINK_PUT_TIMEOUT_KEY -> timeoutInMillis
    }

    val task = new EmsSinkTask(tapRecord)
    try {
      task.start(props.asJava)
      val _ = testCode(connectorName, task, sourceTopic)
    } finally {
      task.stop()
    }
  }
}
