package com.celonis.kafka.connect.transform.fields

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import java.time.Instant
import EmbeddedKafkaMetadataFieldInserter.CelonisOrderFieldName

class EmbeddedKafkaMetadataFieldInserterTest extends org.scalatest.funsuite.AnyFunSuite {
  val timestamp = Instant.now().toEpochMilli
  val schema    = SchemaBuilder.struct().field("f1", Schema.INT64_SCHEMA)
  val struct = {
    val struct = new Struct(schema.build())
    struct.put("f1", 1000L)
    struct
  }

  test("FieldInserter returns a noop if flag is set to false") {
    assertResult(struct) {
      FieldInserter.embeddedKafkaMetadata(doInsert = false, None)
        .insertFields(struct, EmbeddedKafkaMetadata(0, 10, timestamp))
    }
  }
  test("FieldInserter returns a real inserter if flag is set to false but order fields is set to __celonis_order") {
    val kafkaMeta = EmbeddedKafkaMetadata(0, 10, timestamp)
    assertResult(kafkaMeta.offset) {
      val transformedStruct = FieldInserter.embeddedKafkaMetadata(doInsert = false, Some(CelonisOrderFieldName))
        .insertFields(struct, kafkaMeta).asInstanceOf[Struct]
      transformedStruct.get(CelonisOrderFieldName)
    }
  }

  test("fieldInserter adds partition and offset to a non-empty struct") {
    val expected = {
      val updatedSchema =
        schema
          .field("kafkaPartition", Schema.INT32_SCHEMA)
          .field("kafkaOffset", Schema.INT64_SCHEMA)
          .field("kafkaTimestamp", Schema.INT64_SCHEMA)
          .field("kafkaPartitionOffset", Schema.STRING_SCHEMA)
          .field("__celonis_order", Schema.INT64_SCHEMA)

      val exp = new Struct(updatedSchema.build())
      exp.put("kafkaPartition", 1)
      exp.put("kafkaOffset", 101L)
      exp.put("kafkaPartitionOffset", "1_101")
      exp.put("kafkaTimestamp", timestamp)
      exp.put("__celonis_order", 101L)
      exp.put("f1", 1000L)
      exp
    }

    assertResult(expected) {
      FieldInserter.embeddedKafkaMetadata(doInsert = true, None)
        .insertFields(struct, EmbeddedKafkaMetadata(1, 101, timestamp))
    }
  }

  test("fieldInserter adds partition and offset to an empty struct") {
    val schema = SchemaBuilder.struct()
    val struct = new Struct(schema.build())

    val expected = {
      val updatedSchema =
        schema
          .field("kafkaPartition", Schema.INT32_SCHEMA)
          .field("kafkaOffset", Schema.INT64_SCHEMA)
          .field("kafkaTimestamp", Schema.INT64_SCHEMA)
          .field("kafkaPartitionOffset", Schema.STRING_SCHEMA)
          .field("__celonis_order", Schema.INT64_SCHEMA)

      val exp = new Struct(updatedSchema.build())
      exp.put("kafkaPartition", 1)
      exp.put("kafkaOffset", 101L)
      exp.put("kafkaPartitionOffset", "1_101")
      exp.put("kafkaTimestamp", timestamp)
      exp.put("__celonis_order", 101L)
      exp
    }

    assertResult(expected) {
      FieldInserter.embeddedKafkaMetadata(doInsert = true, None)
        .insertFields(struct, EmbeddedKafkaMetadata(1, 101, timestamp))
    }
  }
}
