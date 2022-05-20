package com.celonis.kafka.connect.ems.testcontainers

import com.celonis.kafka.connect.ems.testcontainers.ToxiproxyContainer.defaultTag
import org.testcontainers.containers.{GenericContainer, ToxiproxyContainer => JavaToxiproxyContainer}
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

  def proxy(targetContainer: GenericContainer[_], port: Int): JavaToxiproxyContainer.ContainerProxy = container.getProxy(targetContainer, port)
}

object ToxiproxyContainer {
  private val dockerImage = DockerImageName.parse("shopify/toxiproxy")
  private val defaultTag  = "2.1.0"

  def apply(
    networkAlias: String,
    dockerTag:    String = defaultTag,
  ): ToxiproxyContainer =
    new ToxiproxyContainer(dockerImage, dockerTag, networkAlias)
}
