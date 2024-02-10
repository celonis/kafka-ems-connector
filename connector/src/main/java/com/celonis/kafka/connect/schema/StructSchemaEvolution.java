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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Objects;

/**
 * StructSchemaEvolution is responsible for the recursively merging existing schema and schema from
 * new record. And thus evolving the schema with each new record received from connector.
 */
public class StructSchemaEvolution implements SchemaEvolution {

  /**
   * Merge top level Kafka Connect Structs
   *
   * @param currentSchema existing schema, must be of type Struct
   * @param recordSchema schema of new record, must be of type Struct
   * @return Schema after merging existing and new schema recursively
   */
  @Override
  public Schema evolve(Schema currentSchema, Schema recordSchema) throws SchemaEvolutionException {
    if (currentSchema == recordSchema) return currentSchema;

    // RecordTransformer ensures that the top level schema are of type Struct.
    return mergeSchemas(null, currentSchema, recordSchema);
  }

  /**
   * Merge a Kafka Connect Schemas
   *
   * @param fieldName current field name, `null` when the recursion starts
   * @param currentSchema existing schema, (Accepted types MAP, ARRAY, STRUCT)
   * @param recordSchema schema of new record, (Accepted types MAP, ARRAY, STRUCT)
   * @return Schema after merging existing and new schemas recursively
   */
  private Schema mergeSchemas(String fieldName, Schema currentSchema, Schema recordSchema) {
    // validationsFirst
    validateSchemasTypes(fieldName, currentSchema, recordSchema);

    switch (currentSchema.type()) {
      case STRUCT:
        return mergeStructs(currentSchema, recordSchema);
      case ARRAY:
        return SchemaBuilder.array(
                mergeSchemas(fieldName, currentSchema.valueSchema(), recordSchema.valueSchema()))
            .build();
      case MAP:
        var keySchema =
            mergeSchemas(fieldName, currentSchema.keySchema(), recordSchema.keySchema());
        var valueSchema =
            mergeSchemas(fieldName, currentSchema.valueSchema(), recordSchema.valueSchema());
        return SchemaBuilder.map(keySchema, valueSchema).build();
      default:
        return currentSchema;
    }
  }

  private Schema mergeStructs(Schema currentSchema, Schema recordSchema)
      throws SchemaEvolutionException {
    SchemaBuilder result = SchemaUtils.withMetadata(SchemaBuilder.struct(), currentSchema);

    if (currentSchema.isOptional() || recordSchema.isOptional()) result.optional();

    // First currentSchemaFields
    currentSchema.fields().stream()
        .forEach(
            currentSchemaField -> {
              final var recordSchemaField = recordSchema.field(currentSchemaField.name());
              if (recordSchemaField == null) {
                // If not present in recordSchema, just add it
                result.field(currentSchemaField.name(), currentSchemaField.schema());
              } else {
                // Recursively evolve otherwise
                result.field(
                    currentSchemaField.name(),
                    mergeSchemas(
                        currentSchemaField.name(),
                        currentSchemaField.schema(),
                        recordSchemaField.schema()));
              }
            });

    // Just add remaining record schema fields as they are
    recordSchema.fields().stream()
        .filter(rf -> currentSchema.field(rf.name()) == null)
        .forEach(rf -> result.field(rf.name(), rf.schema()));

    return result.build();
  }

  private void validateSchemasTypes(String fieldName, Schema currentSchema, Schema recordSchema) {
    if (bothPrimitives(currentSchema, recordSchema) && !sameLogicalType(currentSchema, recordSchema)
        || !currentSchema.type().equals(recordSchema.type())) {

      throw new SchemaEvolutionException(
          String.format(
              "New schema has field '%s' with a different type! "
                  + "previous type: %s, current type: %s",
              fieldName, currentSchema, recordSchema));
    }
  }

  private boolean bothPrimitives(Schema s1, Schema s2) {
    return s1.type().isPrimitive() && s2.type().isPrimitive();
  }

  private boolean sameLogicalType(Schema s1, Schema s2) {
    return Objects.equals(s1.type(), s2.type())
        && Objects.equals(s1.name(), s2.name())
        && Objects.equals(s1.version(), s2.version())
        && Objects.equals(s1.parameters(), s2.parameters());
  }
}
