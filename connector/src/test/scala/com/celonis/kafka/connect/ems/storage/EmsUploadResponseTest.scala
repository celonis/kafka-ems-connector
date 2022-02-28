/*
 * Copyright 2017-2022 Celonis Ltd
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
