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

package com.celonis.kafka.connect.ems.testcontainers

import com.celonis.kafka.connect.ems.testcontainers.ToxiproxyContainer.defaultTag
import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.{ ToxiproxyContainer => JavaToxiproxyContainer }
import org.testcontainers.utility.DockerImageName

class ToxiproxyContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String,
) extends SingleContainer[JavaToxiproxyContainer] {
  require(networkAlias != null, "You must define the toxiproxy network alias")

  override val container: JavaToxiproxyContainer =
    new JavaToxiproxyContainer(dockerImage.withTag(dockerTag))

  container.withNetworkAliases(networkAlias)

  def proxy(targetContainer: GenericContainer[_], port: Int, mappedPort: Int): Proxy = {
    val upstream        = targetContainer.getNetworkAliases.get(0) + ":" + port
    val listen          = "0.0.0.0:" + mappedPort
    val toxiproxyClient = new ToxiproxyClient(container.getHost, container.getControlPort)
    toxiproxyClient.createProxy(upstream, listen, upstream)
  }
}

object ToxiproxyContainer {
  private val dockerImage = DockerImageName.parse("ghcr.io/shopify/toxiproxy")
  private val defaultTag  = "2.5.0"

  def apply(
    networkAlias: String,
    dockerTag:    String = defaultTag): ToxiproxyContainer =
    new ToxiproxyContainer(dockerImage, dockerTag, networkAlias)
}
