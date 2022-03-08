/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers

import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse
import org.testcontainers.containers.MockServerContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

trait MockServerEnvironment {

  val network: Network = Network.SHARED

  val mockServerNetworkAlias = "mockserver"

  private val mockserverImage: DockerImageName = DockerImageName.parse("jamesdbloom/mockserver:mockserver-5.5.4")

  val mockServer: MockServerContainer = {
    val msc = new MockServerContainer(mockserverImage)
      .withNetworkAliases(mockServerNetworkAlias)
      .withNetwork(network)
    msc.start()
    msc
  }

  val mockServerClient = new MockServerClient(mockServer.getHost, mockServer.getServerPort)

  def mockServerUrl = s"https://${mockServer.getHost}:${mockServer.getMappedPort(MockServerContainer.PORT)}"

  def mockServerInternalUrl = s"https://$mockServerNetworkAlias:${MockServerContainer.PORT}"

  def withMockResponse(request: HttpRequest, response: HttpResponse)(testCode: => Any): Unit = {
    mockServerClient.when(request).respond(response)
    testCode
    val _ = mockServerClient.reset()
  }
}
