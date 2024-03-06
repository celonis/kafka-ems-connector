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

import com.celonis.kafka.connect.ems.testcontainers.SchemaRegistryContainer.imageName
import com.celonis.kafka.connect.ems.testcontainers.SchemaRegistryContainer.networkAlias
import com.celonis.kafka.connect.ems.testcontainers.SchemaRegistryContainer.port
import com.celonis.kafka.connect.ems.testcontainers.syntax.KafkaContainerOps
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class SchemaRegistryContainer(tag: String, kafkaContainer: KafkaContainer)
    extends GenericContainer[SchemaRegistryContainer](imageName.withTag(tag)) {

  withNetwork(kafkaContainer.getNetwork)
  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forHttp("/subjects").forPort(port).forStatusCode(200))

  withEnv("SCHEMA_REGISTRY_HOST_NAME", networkAlias)
  withEnv("SCHEMA_REGISTRY_LISTENERS", s"http://0.0.0.0:$port")
  withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.getInternalBootstrapServers)

  def getSchemaRegistryUrl: String = s"http://$getHost:${getMappedPort(port)}"

  def getInternalSchemaRegistryUrl: String = s"http://$networkAlias:$port"
}

object SchemaRegistryContainer {
  val imageName:    DockerImageName = DockerImageName.parse("confluentinc/cp-schema-registry")
  val networkAlias: String          = "schema-registry"
  val port:         Int             = 8081
}
