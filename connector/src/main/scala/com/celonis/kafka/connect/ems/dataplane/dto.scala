/*
 * Copyright 2022 Celonis SE
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

package com.celonis.kafka.connect.ems.dataplane

import io.circe.Codec
import io.circe.Json
import io.circe.generic.semiauto.deriveCodec

import java.util.UUID

final case class IngestionSetupRequest(
  dataPoolId:  UUID,
  tableName:   String,
  keySchema:   Option[Json],
  valueSchema: Json,
  partitions:  Option[Int],
  replication: Option[Int],
)

object IngestionSetupRequest {
  implicit val codec: Codec[IngestionSetupRequest] = deriveCodec
}

final case class IngestionSetupResponse(
  topicNames: Map[String, String],
)

object IngestionSetupResponse {
  implicit val codec: Codec[IngestionSetupResponse] = deriveCodec
}

final case class IngressInputMessageAvro(
  key:   Option[String],
  value: Option[String],
)

object IngressInputMessageAvro {
  implicit val codec: Codec[IngressInputMessageAvro] = deriveCodec
}

final case class IngressInputAvro(
  keySchema:   Option[Json],
  valueSchema: Json,
  messages:    List[IngressInputMessageAvro],
)

object IngressInputAvro {
  implicit val codec: Codec[IngressInputAvro] = deriveCodec
}

final case class IngressInputMessageJson(
  key:   Option[Json],
  value: Option[Json],
)

object IngressInputMessageJson {
  implicit val codec: Codec[IngressInputMessageJson] = deriveCodec
}

final case class IngressInputJson(
  messages: List[IngressInputMessageJson],
)

object IngressInputJson {
  implicit val codec: Codec[IngressInputJson] = deriveCodec
}
