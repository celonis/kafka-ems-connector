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

import java.util.Optional;

public class SchemaUtils {
  public static SchemaBuilder withMetadata(SchemaBuilder builder, Schema schema) {
    Optional.ofNullable(schema.parameters())
        .filter(params -> !params.isEmpty())
        .ifPresent(builder::parameters);

    Optional.ofNullable(schema.name()).ifPresent(builder::name);
    Optional.ofNullable(schema.doc()).ifPresent(builder::doc);
    Optional.ofNullable(schema.defaultValue()).ifPresent(builder::defaultValue);
    Optional.ofNullable(schema.version()).ifPresent(builder::version);

    return builder;
  }
}
