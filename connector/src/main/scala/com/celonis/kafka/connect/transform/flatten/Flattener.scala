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

package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenerConfig
import org.apache.kafka.connect.data.Schema

trait Flattener {
  def flatten(value: Any, schema: Option[Schema]): Any
}

object Flattener {
  val noOpFlattener: Flattener = (value: Any, schema: Option[Schema]) => value

  def fromConfig(config: Option[FlattenerConfig]): Flattener =
    config match {
      case Some(config) => config.jsonBlobChunks match {
          case Some(jsonBlobChunks) => new ChunkedJsonBlobFlattener(jsonBlobChunks)
          case None                 => new NormalisingFlattener(new StructFlattener(new SchemaFlattener(config.discardCollections)))
        }
      case None => noOpFlattener
    }
}
