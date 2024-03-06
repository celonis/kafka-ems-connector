/*
 * Copyright 2024 Celonis SE
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
