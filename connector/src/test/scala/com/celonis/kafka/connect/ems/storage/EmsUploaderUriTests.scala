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

import org.http4s.Uri
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URL
import java.net.URLEncoder

class EmsUploaderUriTests extends AnyFunSuite with Matchers {

  private val testClientId        = "CelonisKafka2Ems vx.Test"
  private val testClientIdEncoded = URLEncoder.encode(testClientId, "UTF-8")

  test("creates the URI without the connection") {
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"),
                         "tableA",
                         None,
                         testClientId,
                         None,
                         None,
                         None,
    ) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ClientId}=$testClientIdEncoded",
    ).getOrElse(fail("should parse"))
  }

  test("creates the URI with connection") {
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"),
                         "tableA",
                         Some("c1"),
                         testClientId,
                         None,
                         None,
                         None,
    ) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ConnectionId}=c1&${EmsUploader.ClientId}=$testClientIdEncoded",
    ).getOrElse(fail("should parse the URL"))
  }

  test("creates the URI with the pks") {
    val pks = "a"
    val actual =
      EmsUploader.buildUri(new URL("https://foo.panda.com/api"), "tableA", None, testClientId, None, Some(pks), None)

    val expected = Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ClientId}=$testClientIdEncoded&${EmsUploader.PrimaryKeys}=${URLEncoder.encode(pks, "UTF-8")}",
    ).getOrElse(fail("should parse the URL"))
    actual shouldBe expected
  }

  test("creates the URI with fallback varchar length") {
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"),
                         "tableA",
                         None,
                         testClientId,
                         Some(89),
                         None,
                         None,
    ) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ClientId}=$testClientIdEncoded&${EmsUploader.FallbackVarcharLength}=89",
    ).getOrElse(fail("should parse the URL"))
  }

  test("creates the URI with all params") {
    val pks      = """k1,k2"""
    val sortable = "sortable_abc"
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"),
                         "tableA",
                         Some("connection1"),
                         testClientId,
                         Some(89),
                         Some(pks),
                         Some(sortable),
    ) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ConnectionId}=connection1&${EmsUploader.ClientId}=$testClientIdEncoded&${EmsUploader.FallbackVarcharLength}=89&${EmsUploader.PrimaryKeys}=${URLEncoder.encode(pks,
                                                                                                                                                                                                                                                "UTF-8",
        )}&${EmsUploader.OrderFieldName}=$sortable",
    ).getOrElse(fail("should parse the URL"))
  }
}
