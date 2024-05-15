/*
 * Copyright 2024 Celonis SE
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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public final class StructSchemaAlignment {
  /**
   * Align an object to the schema accumulated by incrementally calling SchemaEvolution#evolve.
   *
   * <p>NOTE: this is needed in order to avoid the unnecessarily frequent flush of output Parquet
   * files due JSON schema inference producing different records for what effectively is the same
   * data (see `EmsOutputRecordSink#put)
   *
   * @param evolvedSchema the schema this value should align to. Must be a superset of the supplied
   *     struct schema.
   * @param value the current SinKRecord input struct
   * @return a struct with the evolved schema
   */
  public static Struct alignTo(Schema evolvedSchema, Struct value) {
    return (Struct) align(evolvedSchema, value);
  }

  private static Object align(Schema evolvedSchema, Object value) {
    switch (evolvedSchema.type()) {
      case ARRAY:
        final var collection = (Collection<?>) value;
        return collection.stream()
            .map(item -> align(evolvedSchema.valueSchema(), item))
            .collect(Collectors.toList());

      case MAP:
        final var map = (Map<?, ?>) value;
        return map.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> align(evolvedSchema.keySchema(), entry.getKey()),
                    entry -> align(evolvedSchema.valueSchema(), entry.getValue())));

      case STRUCT:
        final var structValue = (Struct) value;
        if (structValue.schema() == evolvedSchema) return structValue;
        final var newStruct = new Struct(evolvedSchema);
        final var fieldNamesByLowercaseName = structValue.schema().fields().stream().collect(Collectors.toMap(
                field -> field.name().toLowerCase(),
                Field::name
        ));

        for (final var evolvedField : evolvedSchema.fields()) {
          final var lowerCaseFieldName = evolvedField.name().toLowerCase();
          if (fieldNamesByLowercaseName.get(lowerCaseFieldName) != null) {
            newStruct.put(
                evolvedField.name(),
                align(evolvedField.schema(), structValue.get(fieldNamesByLowercaseName.get(lowerCaseFieldName))));
          }
        }

        return newStruct;
      default:
        return value;
    }
  }
}
