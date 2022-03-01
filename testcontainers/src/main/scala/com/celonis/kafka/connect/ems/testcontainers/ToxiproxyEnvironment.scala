package com.celonis.kafka.connect.ems.testcontainers

import org.testcontainers.containers.MockServerContainer
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.utility.DockerImageName

trait ToxiproxyEnvironment extends MockServerEnvironment {

  val toxiproxyNetworkAlias = "mockserver.celonis.cloud"

  private val toxiproxyImage = DockerImageName.parse("shopify/toxiproxy:2.1.0")

  // Toxiproxy container, which will be used as a TCP proxy
  val toxiproxyContainer: ToxiproxyContainer = {
    val tpc = new ToxiproxyContainer(toxiproxyImage)
      .withNetworkAliases(toxiproxyNetworkAlias)
      .withNetwork(network)
    tpc.start()
    tpc
  }

  val proxy: ToxiproxyContainer.ContainerProxy = toxiproxyContainer.getProxy(mockServer, MockServerContainer.PORT)

  val proxyServerUrl: String = s"https://$toxiproxyNetworkAlias:${proxy.getOriginalProxyPort}"
}
