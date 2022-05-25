/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers.scalatest

import com.celonis.kafka.connect.ems.testcontainers.KafkaConnectContainer
import com.celonis.kafka.connect.ems.testcontainers.SchemaRegistryContainer
import com.celonis.kafka.connect.ems.testcontainers.connect.KafkaConnectClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.TestSuite
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.stream.Collectors
import scala.collection.mutable.ListBuffer

trait KafkaConnectContainerPerSuite extends MockServerContainerPerSuite { this: TestSuite =>

  val confluentPlatformVersion: String = sys.env.getOrElse("CONFLUENT_VERSION", "6.1.0")

  lazy val schemaRegistryInstance: Option[SchemaRegistryContainer] = schemaRegistryContainer()

  lazy val kafkaContainer: KafkaContainer =
    new KafkaContainer(DockerImageName.parse(s"confluentinc/cp-kafka:$confluentPlatformVersion"))
      .withNetwork(network)
      .withNetworkAliases("kafka")
      .withLogConsumer(new Slf4jLogConsumer(log))

  lazy val kafkaConnectContainer: KafkaConnectContainer = {
    KafkaConnectContainer(
      confluentPlatformVersion,
      kafkaContainer          = kafkaContainer,
      schemaRegistryContainer = schemaRegistryContainer(),
      connectPluginJar        = connectPluginJar(),
    ).withLogConsumer(new Slf4jLogConsumer(log))
  }

  // Override for different SchemaRegistryContainer configs
  def schemaRegistryContainer(): Option[SchemaRegistryContainer] = Some(
    new SchemaRegistryContainer(confluentPlatformVersion, kafkaContainer)
      .withLogConsumer(new Slf4jLogConsumer(log)),
  )

  implicit lazy val kafkaConnectClient: KafkaConnectClient = new KafkaConnectClient(kafkaConnectContainer)

  override def beforeAll(): Unit = {
    kafkaContainer.start()
    schemaRegistryInstance.foreach(_.start())
    kafkaConnectContainer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try {
      super.afterAll()
    } finally {
      kafkaConnectContainer.stop()
      schemaRegistryInstance.foreach(_.stop())
      kafkaContainer.stop()
    }

  def connectPluginJar(): Path = {
    val regex = s".*kafka-ems-sink.*.jar"
    val files: java.util.List[Path] = Files.find(
      Paths.get(String.join(File.separator, sys.props("user.dir"), "connector", "target")),
      2,
      (p, _) => p.toFile.getName.matches(regex),
    ).collect(Collectors.toList())
    if (files.isEmpty)
      throw new RuntimeException(s"""Please run `sbt assembly`""")
    files.get(0)
  }

  def createProducer[K, V](keySer: Class[_], valueSer: Class[_]): KafkaProducer[K, V] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, 0)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer)
    schemaRegistryInstance.foreach(s => props.put(SCHEMA_REGISTRY_URL_CONFIG, s.getSchemaRegistryUrl))
    new KafkaProducer[K, V](props)
  }

  def createConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cg-" + UUID.randomUUID())
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer[String, String](props, new StringDeserializer(), new StringDeserializer())
  }

  /**
    * Drain a kafka topic.
    *
    * @param consumer the kafka consumer
    * @param expectedRecordCount the expected record count
    * @tparam K the key type
    * @tparam V the value type
    * @return the records
    */
  def drain[K, V](consumer: KafkaConsumer[K, V], expectedRecordCount: Int): List[ConsumerRecord[K, V]] = {
    val allRecords = ListBuffer[ConsumerRecord[K, V]]()
    eventually {
      consumer.poll(Duration.ofMillis(50))
        .iterator()
        .forEachRemaining(allRecords.addOne)
      assert(allRecords.size == expectedRecordCount)
    }
    allRecords.toList
  }

  lazy val stringAvroProducer: KafkaProducer[String, Any] =
    createProducer(
      classOf[StringSerializer],
      classOf[KafkaAvroSerializer],
    )

  def sendDummyAvroRecord(topic: String): Unit = {
    val valueSchema = SchemaBuilder.record("record").fields()
      .requiredString("a")
      .requiredInt("b")
      .endRecord()

    val record = new GenericData.Record(valueSchema)
    record.put("a", "string")
    record.put("b", 1)

    stringAvroProducer.send(new ProducerRecord(topic, record))
    stringAvroProducer.flush()
  }
}
