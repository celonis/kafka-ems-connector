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

import cats.syntax.either._
import enumeratum._
import okhttp3.OkHttpClient

import java.net.InetSocketAddress
import java.net.Proxy
import java.net.Proxy.{ Type => JavaProxyType }
import java.util.Base64

sealed abstract class ProxyType(val proxyType: JavaProxyType) extends EnumEntry

object ProxyType extends Enum[ProxyType] {

  val values = findValues

  case object Http   extends ProxyType(JavaProxyType.HTTP)
  case object Socks4 extends ProxyType(JavaProxyType.SOCKS)
  case object Socks5 extends ProxyType(JavaProxyType.SOCKS)

}

sealed trait ProxyConfig {
  def createHttpClient():    OkHttpClient
  def headerAuthorization(): Option[String]
}

object ProxyConfig {
  import EmsSinkConfigConstants._

  def extractProxyAuth(props: Map[String, _]): Either[String, Option[BasicAuthentication]] =
    PropertiesHelper.getString(props, PROXY_AUTHENTICATION_KEY) match {
      case Some("BASIC") => {
          for {
            user <- PropertiesHelper.getString(props, PROXY_AUTHBASIC_USERNAME_KEY)
            pass <- PropertiesHelper.getPassword(props, PROXY_AUTHBASIC_PASSWORD_KEY)
          } yield BasicAuthentication(user, pass)
        }.asRight
      case Some(other) => s"Proxy authentication type not currently supported: ($other). Supported values: BASIC".asLeft
      case None        => None.asRight
    }

  def extractProxy(props: Map[String, _]): Either[String, ProxyConfig] = {
    for {
      host <- PropertiesHelper.getString(props, PROXY_HOST_KEY)
      port <- PropertiesHelper.getInt(props, PROXY_PORT_KEY)
      proxyType <- for {
        proxyTypeStringOpt <- PropertiesHelper.getString(props, PROXY_TYPE_KEY)
        proxyType           = ProxyType.withNameInsensitiveOption(proxyTypeStringOpt).getOrElse(ProxyType.Http)
      } yield proxyType
    } yield {
      (host, port, proxyType)
    }
  }
    .map {
      case (host, port, proxyType) => ProxyConfig.extractProxyAuth(props)
          .map(maybeAuth => ConfiguredProxyConfig(host, port, proxyType, maybeAuth))
    }
    .getOrElse(NoProxyConfig().asRight)
}

case class NoProxyConfig() extends ProxyConfig {
  override def createHttpClient(): OkHttpClient = new OkHttpClient()

  override def headerAuthorization(): Option[String] = Option.empty
}

case class ConfiguredProxyConfig(
  host:           String,
  port:           Int,
  proxyType:      ProxyType,
  authentication: Option[BasicAuthentication],
) extends ProxyConfig {

  def createProxyServer(): Proxy = {
    val proxyAddr = new InetSocketAddress(host, port)
    new Proxy(proxyType.proxyType, proxyAddr)
  }

  def createHttpClient(): OkHttpClient =
    new OkHttpClient.Builder().proxy(createProxyServer()).build()

  override def headerAuthorization(): Option[String] = authentication.map(_.encode())

}

case class BasicAuthentication(
  username: String,
  password: String,
) {
  def encode(): String =
    "Basic " + new String(Base64.getEncoder.encode(s"$username:$password".getBytes()))
}
