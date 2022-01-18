/*
 * Copyright 2017-2022 Celonis Ltd
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
