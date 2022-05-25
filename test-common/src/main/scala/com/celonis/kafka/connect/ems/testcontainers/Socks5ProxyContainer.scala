/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers

import com.celonis.kafka.connect.ems.testcontainers.Socks5ProxyContainer.imageName
import com.celonis.kafka.connect.ems.testcontainers.Socks5ProxyContainer.networkAlias
import com.celonis.kafka.connect.ems.testcontainers.Socks5ProxyContainer.port
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

class Socks5ProxyContainer(network: Network, proxyUser: String = "", proxyPassword: String = "")
    extends GenericContainer[Socks5ProxyContainer](imageName) {

  withNetwork(network)
  withNetworkAliases(networkAlias)
  withExposedPorts(port)

  withEnv("PROXY_USER", proxyUser)
  withEnv("PROXY_PASSWORD", proxyPassword)

  def getProxyUrl: String =
    if (proxyUser.isEmpty && proxyPassword.isEmpty) {
      s"$networkAlias:$port"
    } else {
      s"$proxyUser:$proxyPassword@$networkAlias:$port"
    }
}

object Socks5ProxyContainer {
  val imageName:    DockerImageName = DockerImageName.parse("serjs/go-socks5-proxy")
  val networkAlias: String          = "socks5-proxy"
  val port:         Int             = 1080
}
