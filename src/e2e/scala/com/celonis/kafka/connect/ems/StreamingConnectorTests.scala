package com.celonis.kafka.connect.ems

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.CONNECTOR_CLASS_KEY
import com.celonis.kafka.connect.ems.testcontainers.connect.EmsConnectorConfiguration.TOPICS_KEY
import com.celonis.kafka.connect.ems.testcontainers.scalatest.KafkaConnectContainerPerSuite
import com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.mockserver.model.JsonBody
import org.mockserver.verify.VerificationTimes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.concurrent.duration._

class StreamingConnectorTests extends AnyFunSuite with KafkaConnectContainerPerSuite with Matchers {

  val valueSchema = SchemaBuilder.record("record").fields()
    .requiredString("a")
    .requiredInt("b")
    .endRecord()

  private val dataplaneSetupRequest: HttpRequest = HttpRequest.request()
    .withMethod("POST")
    .withPath("/streaming/ingress/api/v1/ingestion/setup")

  private def dataplaneWriteAvroMessagesRequest(topic: String): HttpRequest = HttpRequest.request()
    .withMethod("POST")
    .withPath(s"/streaming/ingress/api/v1/topics/$topic/messages/avro")

  test("send kafka messages to dataplane") {

    val sourceTopic = randomTopicName()
    val emsTable    = randomEmsTable()
    val targetTopic = randomTopicName()
    val dataPoolId = UUID.randomUUID().toString

    val emsConnector = new EmsConnectorConfiguration("ems")
      .withConfig(CONNECTOR_CLASS_KEY, "com.celonis.kafka.connect.ems.sink.EmsStreamingSinkConnector")
      .withConfig(TOPICS_KEY, sourceTopic)
      .withConfig(ENDPOINT_KEY, proxyServerUrl)
      .withConfig(DATA_POOL_ID_KEY, dataPoolId)
      .withConfig(AUTHORIZATION_KEY, "Bearer token")
      .withConfig(TARGET_TABLE_KEY, emsTable)
      .withConfig(VALUE_SCHEMA_KEY, valueSchema.toString)
      .withConfig(COMMIT_RECORDS_KEY, 1)
      .withConfig(COMMIT_SIZE_KEY, 1000000L)
      .withConfig(COMMIT_INTERVAL_KEY, 3600000)
      .withConfig(TMP_DIRECTORY_KEY, "/tmp/")
      .withConfig(ERROR_POLICY_KEY, "THROW")

    mockServerClient.when(dataplaneSetupRequest).respond(
      HttpResponse.response(s"""{ "topicNames": { "$emsTable": "$targetTopic" } }"""),
    )
    mockServerClient.when(dataplaneWriteAvroMessagesRequest(targetTopic))
      .respond(HttpResponse.response().withStatusCode(200))

    withConnector(emsConnector) {

      val record = new GenericData.Record(valueSchema)
      record.put("a", "string")
      record.put("b", 1)

      withStringAvroProducer(_.send(new ProducerRecord(sourceTopic, record)))

      eventually(timeout(60.seconds), interval(1.second)) {
        mockServerClient.verify(
          dataplaneSetupRequest.withBody(
            new JsonBody(
              s"""[
                 |  {
                 |    "dataPoolId": "$dataPoolId",
                 |    "tableName": "$emsTable",
                 |    "valueSchema": ${valueSchema.toString}
                 |  }
                 |]""".stripMargin,
            ),
          ),
          VerificationTimes.once(),
        )
      }

      eventually(timeout(60.seconds), interval(1.seconds)) {

        mockServerClient.verify(dataplaneWriteAvroMessagesRequest(targetTopic).withBody(
          new JsonBody(
            s"""{
               |  "valueSchema": ${valueSchema.toString},
               |  "messages": [
               |    {
               |      "value": "DHN0cmluZwI="
               |    }
               |  ]
               |}""".stripMargin,
          )),
          VerificationTimes.once(),
        )

        val status = kafkaConnectClient.getConnectorStatus(emsConnector.name)
        status.tasks.head.state should be("RUNNING")
      }
    }
  }

}
