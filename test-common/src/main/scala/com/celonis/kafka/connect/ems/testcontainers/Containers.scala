package com.celonis.kafka.connect.ems.testcontainers

import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.containers.output.OutputFrame

import java.util.function.Consumer

abstract class SingleContainer[T <: GenericContainer[_]] {

  def container: T

  def start(): Unit = container.start()

  def stop(): Unit = container.stop()

  def withNetwork(network: Network): this.type = {
    container.withNetwork(network)
    this
  }

  def withExposedPorts(ports: Integer*): this.type = {
    container.withExposedPorts(ports: _*)
    this
  }

  def withLogConsumer(consumer: Consumer[OutputFrame]): this.type = {
    container.withLogConsumer(consumer)
    this
  }
}
