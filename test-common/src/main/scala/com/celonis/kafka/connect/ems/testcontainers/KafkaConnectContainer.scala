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

import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer.jvmCertsPath
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer.networkAlias
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer.pluginPath
import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer.port
import com.celonis.kafka.connect.ems.testcontainers.syntax.KafkaContainerOps
import com.github.dockerjava.api.model.Ulimit
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

class KafkaConnectContainer(
  dockerImage:             DockerImageName,
  kafkaContainer:          KafkaContainer,
  schemaRegistryContainer: Option[SchemaRegistryContainer],
  connectPluginJar:        Path,
) extends GenericContainer[KafkaConnectContainer](dockerImage) {
  require(kafkaContainer != null, "You must define the kafka container")

  withNetwork(kafkaContainer.getNetwork)
  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forHttp("/connectors")
    .forPort(port)
    .forStatusCode(200))
    .withStartupTimeout(Duration.ofSeconds(120))
  withCreateContainerCmdModifier { cmd =>
    val _ = cmd.getHostConfig.withUlimits(Array(new Ulimit("nofile", 65536L, 65536L)))
  }

  // install mockserver ca certificate
  withCopyFileToContainer(MountableFile.forClasspathResource("/mockserver/cacerts"), jvmCertsPath)
  withCopyFileToContainer(MountableFile.forHostPath(connectPluginJar),
                          Paths.get(pluginPath).resolve(connectPluginJar.getFileName).toString,
  )

  if (schemaRegistryContainer.isDefined) {
    withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.getInternalSchemaRegistryUrl)
    withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.getInternalSchemaRegistryUrl)
  } else {
    withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
    withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
  }

  withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias)
  withEnv("CONNECT_REST_PORT", port.toString)
  withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.getInternalBootstrapServers)
  withEnv("CONNECT_GROUP_ID", "connect")
  withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config")
  withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offset")
  withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status")
  withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_PLUGIN_PATH", pluginPath)

  def getConnectRestUrl: String =
    s"http://$getHost:${getMappedPort(port)}"
}

object KafkaConnectContainer {
  val imageName:    DockerImageName = DockerImageName.parse("confluentinc/cp-kafka-connect")
  val networkAlias: String          = "kafka-connect"
  val port:         Int             = 8083
  val pluginPath:   String          = "/connectors"
  val jvmCertsPath = "/usr/lib/jvm/zulu11-ca/lib/security/cacerts"

  def apply(
    confluentPlatformVersion: String,
    kafkaContainer:           KafkaContainer,
    schemaRegistryContainer:  Option[SchemaRegistryContainer] = None,
    connectPluginJar:         Path,
  ): KafkaConnectContainer =
    new KafkaConnectContainer(imageName.withTag(confluentPlatformVersion),
                              kafkaContainer,
                              schemaRegistryContainer,
                              connectPluginJar,
    )
}
