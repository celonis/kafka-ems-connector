/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers

import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer.{jvmCertsPath, networkAlias, pluginPath, port}
import com.celonis.kafka.connect.ems.testcontainers.syntax.KafkaContainerOps
import com.github.dockerjava.api.model.Ulimit
import org.testcontainers.containers.{GenericContainer, KafkaContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.nio.file.{Path, Paths}
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
  val imageName:                               DockerImageName = DockerImageName.parse("confluentinc/cp-kafka-connect")
  val networkAlias:                            String          = "kafka-connect"
  val port:                                    Int             = 8083
  val pluginPath:                              String          = "/connectors"
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
