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

package com.celonis.kafka.connect.ems.testcontainers.connect

import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectClient.ConnectorStatus
import org.apache.http.HttpHeaders
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.json4s._
import org.json4s.native.JsonMethods._
import org.testcontainers.shaded.org.awaitility.Awaitility.await

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

class KafkaConnectClient(kafkaConnectContainer: KafkaConnectContainer) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  val httpClient: HttpClient = {
    val acceptHeader  = new BasicHeader(HttpHeaders.ACCEPT, "application/json")
    val contentHeader = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    HttpClientBuilder.create
      .setDefaultHeaders(List(acceptHeader, contentHeader).asJava)
      .build()
  }

  def registerConnector(connectorConfiguration: EmsConnectorConfiguration, timeoutSeconds: Long = 10L): Unit = {
    val httpPost = new HttpPost(s"${kafkaConnectContainer.getConnectRestUrl}/connectors")
    val entity   = new StringEntity(connectorConfiguration.toJson);
    httpPost.setEntity(entity)
    val response = httpClient.execute(httpPost)
    checkRequestSuccessful(response)
    EntityUtils.consume(response.getEntity)
    await.atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => this.isConnectorConfigured(connectorConfiguration.name))
  }

  def deleteConnector(connectorName: String, timeoutSeconds: Long = 10L): Unit = {
    val httpDelete =
      new HttpDelete(s"${kafkaConnectContainer.getConnectRestUrl}/connectors/$connectorName")
    val response = httpClient.execute(httpDelete)
    checkRequestSuccessful(response)
    EntityUtils.consume(response.getEntity)
    await.atMost(timeoutSeconds, TimeUnit.SECONDS).until(() => !this.isConnectorConfigured(connectorName))
  }

  def getConnectorStatus(connectorName: String): ConnectorStatus = {
    val httpGet  = new HttpGet(s"${kafkaConnectContainer.getConnectRestUrl}/connectors/$connectorName/status")
    val response = httpClient.execute(httpGet)
    checkRequestSuccessful(response)
    val strResponse = EntityUtils.toString(response.getEntity)
    parse(strResponse).extract[ConnectorStatus]
  }

  def isConnectorConfigured(connectorName: String): Boolean = {
    val httpGet  = new HttpGet(s"${kafkaConnectContainer.getConnectRestUrl}/connectors/$connectorName")
    val response = httpClient.execute(httpGet)
    EntityUtils.consume(response.getEntity)
    response.getStatusLine.getStatusCode == 200
  }

  def waitConnectorInRunningState(connectorName: String, timeoutSeconds: Long = 10L): Unit =
    await.atMost(timeoutSeconds, TimeUnit.SECONDS)
      .until(() => Try(getConnectorStatus(connectorName).connector.state.equals("RUNNING")).getOrElse[Boolean](false))

  def checkRequestSuccessful(response: HttpResponse): Unit =
    if (!isSuccess(response.getStatusLine.getStatusCode)) {
      throw new IllegalStateException(s"Http request failed with response: ${EntityUtils.toString(response.getEntity)}")
    }

  def isSuccess(code: Int): Boolean = code / 100 == 2
}

object KafkaConnectClient {
  case class ConnectorStatus(name: String, connector: Connector, tasks: Seq[Tasks], `type`: String)
  case class Connector(state: String, worker_id: String)
  case class Tasks(id: Int, state: String, worker_id: String, trace: Option[String])
}
