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

package com.celonis.kafka.connect.transform

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config.PropertiesHelper
import cats.syntax.either._
import cats.instances.option._
import cats.syntax.apply._

case class FlattenerConfig(
  discardCollections: Boolean                                = false,
  jsonBlobChunks:     Option[FlattenerConfig.JsonBlobChunks] = None,
)

object FlattenerConfig {
  case class JsonBlobChunks(chunks: Int, fallbackVarcharLength: Int)

  def extract(props: Map[String, _], fallbackVarcharLength: Option[Int]): Either[String, Option[FlattenerConfig]] = {
    import PropertiesHelper._
    (getBoolean(props, FLATTENER_ENABLE_KEY).getOrElse(FLATTENER_ENABLE_DEFAULT),
     getBoolean(props, FLATTENER_DISCARD_COLLECTIONS_KEY).getOrElse(FLATTENER_DISCARD_COLLECTIONS_DEFAULT),
     getInt(props, FLATTENER_JSONBLOB_CHUNKS_KEY),
     fallbackVarcharLength,
    ) match {
      case (false, true, _, _) =>
        requiredKeyMissingErrorMsg(FLATTENER_ENABLE_KEY)(FLATTENER_DISCARD_COLLECTIONS_KEY).asLeft
      case (false, _, Some(_), _) =>
        requiredKeyMissingErrorMsg(FLATTENER_ENABLE_KEY)(FLATTENER_JSONBLOB_CHUNKS_KEY).asLeft
      case (_, _, Some(_), None) =>
        requiredKeyMissingErrorMsg(FALLBACK_VARCHAR_LENGTH_KEY)(FLATTENER_JSONBLOB_CHUNKS_KEY).asLeft
      case (true, discardCollections, maybeNumChunks, _) =>
        Some(FlattenerConfig(
          discardCollections,
          (maybeNumChunks, fallbackVarcharLength).mapN(JsonBlobChunks),
        )).asRight
      case _ =>
        None.asRight
    }
  }

  private def requiredKeyMissingErrorMsg(missingKey: String)(key: String) =
    s"Configuration key $key was supplied without setting required key $missingKey . Please supply a value for both keys."

}
