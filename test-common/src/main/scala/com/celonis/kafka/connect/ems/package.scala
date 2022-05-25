/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect

import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse

import java.util.UUID

package object ems {

  val mockEmsResponse: HttpResponse = HttpResponse.response().withBody(
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
      |""".stripMargin,
  )

  def emsRequestForTable(tableName: String, path: String = "/"): HttpRequest = HttpRequest.request()
    .withMethod("POST")
    .withPath(path)
    .withQueryStringParameter("targetName", tableName)

  def randomTopicName(): String = "topic-" + UUID.randomUUID()

  def randomEmsTable(): String = "table-" + UUID.randomUUID()
}
