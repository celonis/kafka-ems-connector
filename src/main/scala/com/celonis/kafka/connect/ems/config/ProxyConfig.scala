/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

case class ProxyConfig(
  host:           String,
  port:           Int,
  authentication: Option[BasicAuthentication],
)

case class BasicAuthentication(
  username: String,
  password: String,
)
