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

  val port: Int = JavaMockServerContainer.PORT

  override val container: JavaMockServerContainer =
    new JavaMockServerContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def mockServerClient = new MockServerClient(container.getHost, container.getServerPort)
  }

}

object MockServerContainer {
  private val dockerImage         = DockerImageName.parse("mockserver/mockserver")
  private val defaultTag          = "5.15.0"
  private val defaultNetworkAlias = "mockserver"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag): MockServerContainer =
    new MockServerContainer(dockerImage, dockerTag, networkAlias)
}
