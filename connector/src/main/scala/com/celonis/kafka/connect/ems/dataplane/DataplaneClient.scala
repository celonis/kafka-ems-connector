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

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.kernel.Resource
import com.celonis.kafka.connect.ems.config.AuthorizationHeader
import org.http4s.Header
import org.http4s.Method
import org.http4s.Uri
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.typelevel.ci.CIString

trait DataplaneClient[F[_]] {
  def setup(request: NonEmptyList[IngestionSetupRequest]): F[IngestionSetupResponse]

  def writeMessagesJson(topicName: String, input: IngressInputJson): F[Unit]

  def writeMessagesAvro(topicName: String, input: IngressInputAvro): F[Unit]
}

object DataplaneClient {

  def apply[F[_]: Async](httpClient: Client[F], authorizationHeader: AuthorizationHeader, baseUri: Uri) = {

    val dsl = org.http4s.client.dsl.Http4sClientDsl[F]
    import dsl._
    import org.http4s.circe.CirceEntityCodec._

    val authorization = Header.Raw(CIString("Authorization"), authorizationHeader.header)

    new DataplaneClient[F] {
      override def setup(request: NonEmptyList[IngestionSetupRequest]): F[IngestionSetupResponse] = {
        val uri = baseUri / "streaming" / "ingress" / "api" / "v1" / "ingestion" / "setup"
        httpClient.expect[IngestionSetupResponse](Method.POST(request, uri, authorization))
      }

      override def writeMessagesJson(topicName: String, input: IngressInputJson): F[Unit] = {
        val uri = baseUri / "streaming" / "ingress" / "api" / "v1" / "topics" / topicName / "messages"
        httpClient.expect[Unit](Method.POST(input, uri, authorization))
      }

      override def writeMessagesAvro(topicName: String, input: IngressInputAvro): F[Unit] = {
        val uri = baseUri / "streaming" / "ingress" / "api" / "v1" / "topics" / topicName / "messages" / "avro"
        httpClient.expect[Unit](Method.POST(input, uri, authorization))
      }
    }
  }

  def resource[F[_]: Async](authorizationHeader: AuthorizationHeader, baseUri: Uri): Resource[F, DataplaneClient[F]] =
    BlazeClientBuilder[F].resource
      .map(apply(_, authorizationHeader, baseUri))

}
