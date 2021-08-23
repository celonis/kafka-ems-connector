/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.storage

import org.http4s.Uri
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.net.URL

class EmsUploaderUriTests extends AnyFunSuite with Matchers {
  test("creates the URI without the connection") {
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"), "tableA", None) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA",
    ).getOrElse(fail("should parse"))
  }

  test("creates the URI with connection") {
    EmsUploader.buildUri(new URL("https://foo.panda.com/api"), "tableA", Some("c1")) shouldBe Uri.fromString(
      s"https://foo.panda.com/api?${EmsUploader.TargetTable}=tableA&${EmsUploader.ConnectionId}=c1",
    ).getOrElse(fail("should parse the URL"))
  }
}
