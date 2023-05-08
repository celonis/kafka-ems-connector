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

package com.celonis.kafka.connect.ems.config

import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CLOSE_EVERY_CONNECTION_DEFAULT_VALUE
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CLOSE_EVERY_CONNECTION_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_POOL_KEEPALIVE_MILLIS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.CONNECTION_POOL_MAX_IDLE_CONNECTIONS_KEY
import okhttp3.ConnectionPool

import java.util.concurrent.TimeUnit

case class PoolingConfig(maxIdleConnections: Int, keepAliveMillis: Long, closeConn: Boolean) {
  def toConnectionPool() = new ConnectionPool(maxIdleConnections, keepAliveMillis, TimeUnit.MILLISECONDS)
}

object PoolingConfig {
  def extract(props: Map[String, _]): PoolingConfig = {
    val idles: Int = PropertiesHelper.getInt(props, CONNECTION_POOL_MAX_IDLE_CONNECTIONS_KEY).getOrElse(
      CONNECTION_POOL_MAX_IDLE_CONNECTIONS_DEFAULT_VALUE,
    )
    val keepAlive: Long = PropertiesHelper.getLong(props, CONNECTION_POOL_KEEPALIVE_MILLIS_KEY).getOrElse(
      CONNECTION_POOL_KEEPALIVE_MILLIS_DEFAULT_VALUE,
    )
    val closeConn: Boolean = PropertiesHelper.getBoolean(props, CLOSE_EVERY_CONNECTION_KEY).getOrElse(
      CLOSE_EVERY_CONNECTION_DEFAULT_VALUE,
    )

    PoolingConfig(idles, keepAlive, closeConn)
  }
}
