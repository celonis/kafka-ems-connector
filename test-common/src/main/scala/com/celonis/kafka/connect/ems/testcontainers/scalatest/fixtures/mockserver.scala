/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.testcontainers.scalatest.fixtures

import org.mockserver.client.MockServerClient
import org.mockserver.model.HttpRequest
import org.mockserver.model.HttpResponse

object mockserver {

  def withMockResponse(
    request:  HttpRequest,
    response: HttpResponse,
  )(testCode: => Any,
  )(
    implicit
    mockServerClient: MockServerClient,
  ): Unit =
    try {
      mockServerClient.when(request).respond(response)
      val _ = testCode
    } finally {
      val _ = mockServerClient.reset()
    }
}
