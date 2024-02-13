/*
 * Copyright 2023 Celonis SE
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
package com.celonis.kafka.connect.schema;

import com.celonis.kafka.connect.schema.StructSchemaEvolutionTest.LogicalTypeScenario
import io.confluent.connect.avro.AvroData
import org.apache.avro.LogicalTypes
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.Inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try

class StructSchemaEvolutionTest extends AnyFunSuite with Matchers with Inside {
  private val schemaEv = new StructSchemaEvolution();

  test("schema evolution with common primitive fields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()

    val expectedResult =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()

    val result = schemaEv.evolve(currentSchema, recordSchema)

    expectedResult shouldEqual result
  }

  test("throws exception For Different SchemaTypes with same name") {
    val currentSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build();
    val recordSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.INT64_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build();

    val result = Try(schemaEv.evolve(currentSchema, recordSchema))

    inside(result) {
      case Failure(exception) =>
        exception.isInstanceOf[SchemaEvolutionException] shouldBe true
        exception.getMessage.contains("New schema has field 'a_string' with a different type!") shouldBe true
    }
  }
  test("schema evolution with empty current fields") {
    val currentSchema = SchemaBuilder.struct().name("test-schema").version(1).build();
    val recordSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()

    val expectedResult =
      SchemaBuilder.struct()
        .name("test-schema")
        .version(1)
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()

    val result = schemaEv.evolve(currentSchema, recordSchema);

    result shouldEqual expectedResult
  }
  test("SchemaEvolutionWithEmptyRecordFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()
    val recordSchema = SchemaBuilder.struct().build()

    val expectedResult =
      SchemaBuilder.struct()
        .field("a_string", Schema.STRING_SCHEMA)
        .field("an_int", Schema.INT64_SCHEMA)
        .build()

    schemaEv.evolve(currentSchema, recordSchema) shouldEqual expectedResult
  }

  test("SchemaEvolutionWithCommonStructFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()

    val expectedResult =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build();

    schemaEv.evolve(currentSchema, recordSchema) shouldEqual expectedResult
  }
  test("SchemaEvolutionWithAdditionalRecordFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct()
            .field("a_string", Schema.STRING_SCHEMA)
            .field("an_int", Schema.INT64_SCHEMA)
            .field("a_string_2", Schema.STRING_SCHEMA)
            .build(),
        )
        .build()

    val expectedResult =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct()
            .field("a_string", Schema.STRING_SCHEMA)
            .field("an_int", Schema.INT64_SCHEMA)
            .field("a_string_2", Schema.STRING_SCHEMA)
            .build(),
        )
        .build()

    schemaEv.evolve(currentSchema, recordSchema) shouldEqual expectedResult
  }

  test("testSchemaEvolutionPreservesFieldOptionality") {
    val currentSchema =
      SchemaBuilder.struct.field("a_struct",
                                 SchemaBuilder.struct.field("optional_string", Schema.OPTIONAL_STRING_SCHEMA).build,
      ).field("an_empty_struct", SchemaBuilder.struct.optional.schema).build
    val recordSchema = SchemaBuilder.struct.field(
      "a_struct",
      SchemaBuilder.struct.optional.field("optional_string", Schema.OPTIONAL_STRING_SCHEMA).field("an_int",
                                                                                                  Schema.INT64_SCHEMA,
      ).field("a_string_2", Schema.STRING_SCHEMA).build,
    ).field("an_empty_struct", SchemaBuilder.struct.schema).build
    val expectedResult = SchemaBuilder.struct.field(
      "a_struct",
      SchemaBuilder.struct.optional.field("optional_string", Schema.OPTIONAL_STRING_SCHEMA).field("an_int",
                                                                                                  Schema.INT64_SCHEMA,
      ).field("a_string_2", Schema.STRING_SCHEMA).build,
    ).field("an_empty_struct", SchemaBuilder.struct.optional.schema).build
    val result = schemaEv.evolve(currentSchema, recordSchema)
    result shouldEqual expectedResult
  }

  test("SchemaEvolutionWithAdditionalStructRecordField") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_3",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()

    val expectedResult =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_3",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build()

    val result = schemaEv.evolve(currentSchema, recordSchema);

    for (field <- expectedResult.fields().asScala) {
      field.schema() shouldEqual result.field(field.name()).schema()
    }

  }
  test("SchemaEvolutionWithAdditionalStructCurrentField") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_3",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .build();
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_4",
          SchemaBuilder.struct().field("an_int", Schema.INT64_SCHEMA).build(),
        )
        .field(
          "a_struct_3",
          SchemaBuilder.struct()
            .field("a_string", Schema.STRING_SCHEMA)
            .field("an_int", Schema.INT64_SCHEMA)
            .build(),
        )
        .build();

    val expectedResult =
      SchemaBuilder.struct()
        .field(
          "a_struct",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_2",
          SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA).build(),
        )
        .field(
          "a_struct_4",
          SchemaBuilder.struct().field("an_int", Schema.INT64_SCHEMA).build(),
        )
        .field(
          "a_struct_3",
          SchemaBuilder.struct()
            .field("a_string", Schema.STRING_SCHEMA)
            .field("an_int", Schema.INT64_SCHEMA)
            .build(),
        )
        .build();

    val result = schemaEv.evolve(currentSchema, recordSchema);
    for (field <- expectedResult.fields().asScala) {
      field.schema() shouldEqual result.field(field.name()).schema()
    }
  }
  test("ArrayEvolutionWithDifferentStructFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA)),
        )
        .build()

    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(SchemaBuilder.struct().field("an_int", Schema.INT64_SCHEMA)),
        )
        .build()

    val result = schemaEv.evolve(currentSchema, recordSchema);
    val expectedSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(
            SchemaBuilder.struct()
              .field("a_string", Schema.STRING_SCHEMA)
              .field("an_int", Schema.INT64_SCHEMA)
              .build(),
          )
            .build(),
        )
        .build()

    result shouldEqual expectedSchema
  }
  test("RecursiveArrayEvolutionWithDifferentStructFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(
            SchemaBuilder.array(
              SchemaBuilder.struct().field("a_string", Schema.STRING_SCHEMA),
            ),
          ),
        )
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(
            SchemaBuilder.array(
              SchemaBuilder.struct().field("an_int", Schema.INT64_SCHEMA),
            ),
          ),
        )
        .build()

    val expectedSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.array(
            SchemaBuilder.array(
              SchemaBuilder.struct()
                .field("a_string", Schema.STRING_SCHEMA)
                .field("an_int", Schema.INT64_SCHEMA)
                .build(),
            )
              .build(),
          )
            .build(),
        )
        .build()

    schemaEv.evolve(currentSchema, recordSchema) shouldEqual expectedSchema
  }
  test("MapEvolutionWithDifferentStructFields") {
    val currentSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.map(
            SchemaBuilder.struct().field("key_string", Schema.STRING_SCHEMA),
            SchemaBuilder.struct().field("value_string", Schema.STRING_SCHEMA),
          ),
        )
        .build()
    val recordSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.map(
            SchemaBuilder.struct().field("key_int", Schema.INT64_SCHEMA),
            SchemaBuilder.struct().field("value_int", Schema.INT64_SCHEMA),
          ),
        )
        .build()

    val expectedSchema =
      SchemaBuilder.struct()
        .field(
          "topStruct",
          SchemaBuilder.map(
            SchemaBuilder.struct()
              .field("key_string", Schema.STRING_SCHEMA)
              .field("key_int", Schema.INT64_SCHEMA)
              .build(),
            SchemaBuilder.struct()
              .field("value_string", Schema.STRING_SCHEMA)
              .field("value_int", Schema.INT64_SCHEMA)
              .build(),
          )
            .build(),
        )
        .build()

    schemaEv.evolve(currentSchema, recordSchema) shouldEqual expectedSchema
  }
  test("subSchemaMatchesItsPreviousVersion") {
    Seq("x", "y", "z").foreach { field =>
      val schema1 =
        SchemaBuilder.struct()
          .field("x", Schema.INT32_SCHEMA)
          .field("y", Schema.INT32_SCHEMA)
          .field("z", Schema.INT32_SCHEMA)
          .build()

      val schema2 = SchemaBuilder.struct().field(field, Schema.INT32_SCHEMA).build()

      schemaEv.evolve(schema1, schema2) shouldEqual schema1
    }
  }
  test("fieldOrderIsPreserved") {
    val prefix1 = "s1_"
    val prefix2 = "s2_"

    val schema1 = withAToZFields(SchemaBuilder.struct(), prefix1).build()
    val schema2 = withAToZFields(SchemaBuilder.struct(), prefix2).build()

    val expected =
      withAToZFields(withAToZFields(SchemaBuilder.struct(), prefix1), prefix2).build()

    schemaEv.evolve(schema1, schema2) shouldEqual expected

  }

  def withAToZFields(b: SchemaBuilder, prefix: String): SchemaBuilder = {
    for (char <- 'a' to 'z') {
      b.field(prefix + char, Schema.BYTES_SCHEMA);
    }
    b
  }

  test("avroLogicalTypeAwareness") {
    for (
      scenario <- Seq(
        LogicalTypeScenario.bytesNeqDecimal,
        LogicalTypeScenario.decimalPrecisionScale,
        LogicalTypeScenario.date,
        LogicalTypeScenario.timestamp,
        LogicalTypeScenario.time,
      )
    ) {
      val fieldName = "some-field";

      val previousSchema =
        SchemaBuilder.struct().field(fieldName, scenario.previousSchema).build()

      val currentSchema =
        SchemaBuilder.struct().field(fieldName, scenario.currentSchema).build()

      for (
        (previous, current) <- Map(
          previousSchema -> currentSchema,
          currentSchema  -> previousSchema,
        )
      ) {

        val result = Try(schemaEv.evolve(previous, current))
        inside(result) {
          case Failure(exception) =>
            exception.isInstanceOf[SchemaEvolutionException] shouldBe true
            if (scenario.current.getLogicalType != null) {
              exception.getMessage.contains(scenario.currentSchema.name()) shouldBe true
            }
            if (scenario.previous.getLogicalType != null) {
              exception.getMessage.contains(scenario.previousSchema.name()) shouldBe true
            }
        }

      }
    }
  }
}
object StructSchemaEvolutionTest {
  case class LogicalTypeScenario(
    label:    String,
    previous: org.apache.avro.Schema,
    current:  org.apache.avro.Schema,
  ) {

    private val converter = new AvroData(100);

    override def toString: String = label

    def previousSchema: Schema = converter.toConnectSchema(previous)

    def currentSchema: Schema = converter.toConnectSchema(current)
  }

  object LogicalTypeScenario {
    private def avroBuilder(): org.apache.avro.SchemaBuilder.TypeBuilder[org.apache.avro.Schema] =
      org.apache.avro.SchemaBuilder.builder()

    val bytesNeqDecimal: LogicalTypeScenario = {

      val logicalType = LogicalTypes.decimal(5, 2).addToSchema(avroBuilder().bytesType())

      LogicalTypeScenario(
        label    = "BYTES != AVRO Decimal(5,3)",
        previous = avroBuilder().bytesType(),
        current  = avroBuilder().`type`(logicalType),
      )
    }

    val decimalPrecisionScale: LogicalTypeScenario = {
      val logicalType5_2 = LogicalTypes.decimal(5, 2).addToSchema(avroBuilder().bytesType())
      val logicalType5_3 = LogicalTypes.decimal(5, 3).addToSchema(avroBuilder().bytesType())
      val decimal5_2     = avroBuilder().`type`(logicalType5_2)
      val decimal5_3     = avroBuilder().`type`(logicalType5_3)
      LogicalTypeScenario(
        label    = "AVRO Decimal(5,2) != AVRO Decimal(5,3)",
        previous = decimal5_2,
        current  = decimal5_3,
      )
    }

    val date: LogicalTypeScenario = {
      val logicalType =
        LogicalTypes.date().addToSchema(avroBuilder().intType()) // days since epoch

      val aDate = avroBuilder().`type`(logicalType)
      LogicalTypeScenario(
        label    = "INT != AVRO DATE",
        previous = avroBuilder().intType(),
        current  = aDate,
      )
    }

    val timestamp: LogicalTypeScenario = {
      val logicalType = LogicalTypes.timestampMillis().addToSchema(avroBuilder().longType())

      val ts = avroBuilder().`type`(logicalType)
      LogicalTypeScenario(
        label    = "LONG != AVRO TIMESTAMP",
        previous = avroBuilder().longType(),
        current  = ts,
      )
    }

    val time: LogicalTypeScenario = {
      val logicalType = LogicalTypes.timeMillis().addToSchema(avroBuilder().intType())

      val t = avroBuilder().`type`(logicalType)

      LogicalTypeScenario(
        label    = "INT != AVRO TIME",
        previous = avroBuilder().intType(),
        current  = t,
      )
    }

  }
}
