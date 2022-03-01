/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectEnvironment
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.verify.VerificationTimes
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration.DurationInt

class SmokeTests extends AnyFunSuite with KafkaConnectEnvironment with Matchers {

  test("upload test") {

    val sourceTopic = "topic-" + UUID.randomUUID()

    val emsRequest = request()
      .withPath("/")
      .withQueryStringParameter("targetName", "ems-table")

    // mock ems response
    mockServerClient.when(emsRequest)
      .respond(
        response().withBody(
          """
            |{
            |  "id" : "7513bc96-7003-480e-9b72-4e4ae8a7b05c",
            |  "fileName" : "my-parquet-nice-file-name.parquet",
            |  "bucketId" : "6ca836f9-12e3-46f0-a5c4-20c9a309833d",
            |  "flushStatus" : "NEW",
            |  "clientId" : null,
            |  "fallbackVarcharLength" : 10000,
            |  "upsertStrategy" : null
            |}
            |""".stripMargin,
        ),
      )

    // register connector
    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(AUTHORIZATION_KEY, "AppKey key")
      .withConfig(TARGET_TABLE_KEY, "ems-table")
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")

    kafkaConnectClient.registerConnector(emsConnector)
    kafkaConnectClient.waitConnectorInRunningState(emsConnector.name)

    // produce a record to the source topic
    val valueSchema = SchemaBuilder.record("record").fields()
      .requiredString("a")
      .requiredInt("b")
      .endRecord()

    val record = new GenericData.Record(valueSchema)
    record.put("a", "string")
    record.put("b", 1)
    stringAvroProducer.send(new ProducerRecord(sourceTopic, record))
    stringAvroProducer.flush()

    // assert
    eventually(timeout(60 seconds), interval(1 seconds)) {
      mockServerClient.verify(emsRequest, VerificationTimes.once())
    }

    val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
    status.tasks.head.state should be("RUNNING")

    kafkaConnectClient.deleteConnector(emsConnector)
  }
}
