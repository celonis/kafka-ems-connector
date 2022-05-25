/*
 * Copyright 2017-2022 Celonis Ltd
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
