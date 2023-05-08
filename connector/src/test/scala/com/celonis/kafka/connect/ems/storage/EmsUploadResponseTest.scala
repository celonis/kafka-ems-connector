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

package com.celonis.kafka.connect.ems.storage

import io.circe.parser._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EmsUploadResponseTest extends AnyFunSuite with Matchers with EitherValues {

  test("should decode valid emsUploadResponse") {

    val jsonResponse =
      """
        |{
        |  "id" : "7513bc96-7003-480e-9b72-4e4ae8a7b05c",
        |  "fileName" : "my-parquet-nice-file-name.parquet",
        |  "bucketId" : "6ca836f9-12e3-46f0-a5c4-20c9a309833d",
        |  "flushStatus" : "NEW",
        |  "clientId" : null,
        |  "fallbackVarcharLength" : 10000,
        |  "upsertStrategy" : null
        |}
        |""".stripMargin

    val emsUploadResponse = decode[EmsUploadResponse](jsonResponse)
    emsUploadResponse.value should be(
      EmsUploadResponse(
        id                    = "7513bc96-7003-480e-9b72-4e4ae8a7b05c",
        fileName              = "my-parquet-nice-file-name.parquet",
        bucketId              = "6ca836f9-12e3-46f0-a5c4-20c9a309833d",
        flushStatus           = "NEW",
        clientId              = None,
        fallbackVarcharLength = Some(10000),
        upsertStrategy        = None,
      ),
    )

  }
}
