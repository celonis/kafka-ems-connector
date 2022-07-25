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

package com.celonis.kafka.connect.ems.config

import cats.effect.Resource
import cats.effect.kernel.Async
import okhttp3.OkHttpClient
import org.http4s.client.Client
import org.http4s.okhttp.client.OkHttpBuilder

object ProxyIO {

  def createHttpClient[F[_]](
    okHttpClient: OkHttpClient,
  )(
    implicit
    A: Async[F],
  ): Resource[F, Client[F]] =
    OkHttpBuilder[F](okHttpClient).resource

}
