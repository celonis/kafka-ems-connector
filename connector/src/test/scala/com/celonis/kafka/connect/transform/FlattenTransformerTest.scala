/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class FlattenTransformerTest extends AnyFunSuite with Matchers with OptionValues with LazyLogging {
  private val smt = new EmsFlattenTransformer[SinkRecord]()

  smt.configure(
    scala.collection.Map[String, Any]().asJava,
  )

  private val Topic     = "MyTopic"
  private val Partition = 1
  private val Offset    = 100L
  private val KeySchema = SchemaBuilder.bool().build()
  private val Key       = true

  private val OriginInfoSchema: Schema = SchemaBuilder.struct()
    .optional()
    .field("producerId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("kafkaOffset", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  private val ProcessEventSchema: Schema = SchemaBuilder.struct()
    .field("caseId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("activity", Schema.OPTIONAL_STRING_SCHEMA)
    .field("startTime", Schema.OPTIONAL_STRING_SCHEMA)
    .field("confidence", Schema.OPTIONAL_FLOAT64_SCHEMA)
    .field("originInfo", OriginInfoSchema)
    .field("replaces", Schema.OPTIONAL_INT32_SCHEMA)
    .field("sensorIds", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
    .build()

  private val TestRecordSchema = SchemaBuilder.struct()
    .field("processEvent", ProcessEventSchema)
    .build()

  private val OriginInfoStruct = new Struct(OriginInfoSchema)
    .put("producerId", "simple-process-step-detector")
    .put("kafkaOffset", "178095272")

  private val ProcessEventStruct = new Struct(ProcessEventSchema)
    .put("caseId", "12885321")
    .put("activity", "Arbeitsvorbereitung")
    .put("startTime", "2022-03-07T12:49:24.864983Z")
    .put("confidence", 0.8249308)
    .put("originInfo", OriginInfoStruct)
    .put("replaces", 44745)
    .put("sensorIds", List("3c69").asJava)

  private val EvilNullsStruct = new Struct(ProcessEventSchema)
    .put("caseId", null)
    .put("activity", null)
    .put("startTime", null)
    .put("confidence", null)
    .put("originInfo", OriginInfoStruct)
    .put("replaces", null)
    .put("sensorIds", List[String]().asJava)

  private val TestRecord = new Struct(TestRecordSchema)
    .put("processEvent", ProcessEventStruct)

  private val TestNullRecord = new Struct(TestRecordSchema)
    .put("processEvent", EvilNullsStruct)

  case class KCField(name: String, schemaType: Schema.Type)

  object KCField {
    def apply(f: Field): KCField =
      KCField(f.name(), f.schema().`type`())
  }

  test("should flatten arrays within a struct") {
    val record: SinkRecord = new SinkRecord(Topic,
                                            Partition,
                                            KeySchema,
                                            Key,
                                            TestRecordSchema,
                                            TestRecord,
                                            Offset,
                                            1641566630831L,
                                            TimestampType.CREATE_TIME,
    )
    val transformed = smt.apply(record)
    checkCommonFields(transformed)
    val sch = transformed.valueSchema()
    sch should not be null
    sch.fields.size() should be(8)
    KCField(sch.fields.get(0)) should be(KCField("processEvent_caseId", Schema.Type.STRING))
    KCField(sch.fields.get(1)) should be(KCField("processEvent_activity", Schema.Type.STRING))
    KCField(sch.fields.get(2)) should be(KCField("processEvent_startTime", Schema.Type.STRING))
    KCField(sch.fields.get(3)) should be(KCField("processEvent_confidence", Schema.Type.FLOAT64))
    KCField(sch.fields.get(4)) should be(KCField("processEvent_originInfo_producerId", Schema.Type.STRING))
    KCField(sch.fields.get(5)) should be(KCField("processEvent_originInfo_kafkaOffset", Schema.Type.STRING))
    KCField(sch.fields.get(6)) should be(KCField("processEvent_replaces", Schema.Type.INT32))
    KCField(sch.fields.get(7)) should be(KCField("processEvent_sensorIds", Schema.Type.STRING))

    transformed.value().toString should be(
      "Struct{processEvent_caseId=12885321,processEvent_activity=Arbeitsvorbereitung,processEvent_startTime=2022-03-07T12:49:24.864983Z,processEvent_confidence=0.8249308,processEvent_originInfo_producerId=simple-process-step-detector,processEvent_originInfo_kafkaOffset=178095272,processEvent_replaces=44745,processEvent_sensorIds=[\"3c69\"]}",
    )
  }

  test("should operate correctly with null values present") {
    val record: SinkRecord = new SinkRecord(Topic,
                                            Partition,
                                            KeySchema,
                                            Key,
                                            TestRecordSchema,
                                            TestNullRecord,
                                            Offset,
                                            1641566630831L,
                                            TimestampType.CREATE_TIME,
    )
    val transformed = smt.apply(record)
    checkCommonFields(transformed)

    transformed.value().toString should be(
      "Struct{processEvent_originInfo_producerId=simple-process-step-detector,processEvent_originInfo_kafkaOffset=178095272,processEvent_sensorIds=[]}",
    )
  }

  test("performs no transformation when value is a primitive") {
    List(
      Schema.OPTIONAL_STRING_SCHEMA                                  -> null,
      Schema.OPTIONAL_STRING_SCHEMA                                  -> "hello",
      Schema.INT32_SCHEMA                                            -> 123,
      Schema.OPTIONAL_BOOLEAN_SCHEMA                                 -> true,
      SchemaBuilder.array(Schema.STRING_SCHEMA)                      -> Array("a", "b", "c"),
      SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT32_SCHEMA) -> Map("x" -> 5.5f).asJava,
    ).foreach {
      case (schema, value) =>
        val record: SinkRecord = new SinkRecord(Topic,
                                                Partition,
                                                KeySchema,
                                                Key,
                                                schema,
                                                value,
                                                Offset,
                                                1641566630831L,
                                                TimestampType.CREATE_TIME,
        )
        val transformed = smt.apply(record)
        checkCommonFields(transformed)
        assertResult(value)(transformed.value())
    }
  }

  private def checkCommonFields(transformed: SinkRecord) = {
    transformed.topic() should be(Topic)
    transformed.kafkaPartition() should be(Partition)
    transformed.kafkaOffset() should be(Offset)
    transformed.keySchema() should be(KeySchema)
    transformed.key() shouldBe Key
  }
}
