/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers.connect

import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectContainer.imageName
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectContainer.jvmCertsPath
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectContainer.networkAlias
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectContainer.pluginPath
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectContainer.port
import com.celonis.kafka.connect.ems.testcontainers.sr.SchemaRegistryContainer
import com.celonis.kafka.connect.ems.testcontainers.syntax.KafkaContainerOps
import com.github.dockerjava.api.model.Ulimit
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

import java.nio.file.Path
import java.time.Duration

class KafkaConnectContainer(
  tag:                     String,
  kafkaContainer:          KafkaContainer,
  schemaRegistryContainer: SchemaRegistryContainer,
  connectorPath:           Path,
) extends GenericContainer[KafkaConnectContainer](imageName.withTag(tag)) {

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
  withCopyFileToContainer(MountableFile.forHostPath(connectorPath), pluginPath)

  withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias)
  withEnv("CONNECT_REST_PORT", port.toString)
  withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.getInternalBootstrapServers)
  withEnv("CONNECT_GROUP_ID", "connect")
  withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
  withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.getInternalSchemaRegistryUrl)
  withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
  withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.getInternalSchemaRegistryUrl)
  withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config")
  withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offset")
  withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status")
  withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
  withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
  withEnv("CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
  withEnv("CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
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
}
