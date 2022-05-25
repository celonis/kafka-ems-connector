/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers

import com.celonis.kafka.connect.ems.testcontainers.MockServerContainer.defaultNetworkAlias
import com.celonis.kafka.connect.ems.testcontainers.MockServerContainer.defaultTag
import org.mockserver.client.MockServerClient
import org.testcontainers.containers.{ MockServerContainer => JavaMockServerContainer }
import org.testcontainers.utility.DockerImageName

class MockServerContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaMockServerContainer] {

  val port: Int = 1080

  override val container: JavaMockServerContainer =
    new JavaMockServerContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def mockServerClient = new MockServerClient(container.getHost, container.getServerPort)

    def mockServerUrl: String = s"https://$networkAlias:${container.getServerPort}"
  }

  def mockServerUrl = s"https://$networkAlias:$port"
}

object MockServerContainer {
  private val dockerImage         = DockerImageName.parse("jamesdbloom/mockserver")
  private val defaultTag          = "mockserver-5.5.4"
  private val defaultNetworkAlias = "mockserver"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): MockServerContainer =
    new MockServerContainer(dockerImage, dockerTag, networkAlias)
}
