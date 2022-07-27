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
import okhttp3.ConnectionPool
import okhttp3.Credentials
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.Route
import okhttp3.{ Authenticator => OkHttpAuthenticator }

import java.net.InetSocketAddress
import java.net.PasswordAuthentication
import java.net.Proxy
import java.net.Proxy.{ Type => JavaProxyType }
import java.net.{ Authenticator => JavaAuthenticator }
import java.util.Base64
import java.util.concurrent.TimeUnit

case class BasicAuthentication(
  username: String,
  password: String,
) {
  def encode(): String =
    "Basic " + new String(Base64.getEncoder.encode(s"$username:$password".getBytes()))
}

sealed abstract class ProxyType(val javaProxyType: JavaProxyType) extends EnumEntry

object ProxyType extends Enum[ProxyType] {

  val values = findValues

  case object Http   extends ProxyType(JavaProxyType.HTTP)
  case object Socks4 extends ProxyType(JavaProxyType.SOCKS)
  case object Socks5 extends ProxyType(JavaProxyType.SOCKS)
}

sealed trait ProxyConfig {
  def createHttpClient():    OkHttpClient
  def authorizationHeader(): Option[String]
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
  override def createHttpClient(): OkHttpClient =
    new OkHttpClient.Builder().connectionPool(new ConnectionPool(0, 500, TimeUnit.MILLISECONDS)).build()

  override def authorizationHeader(): Option[String] = Option.empty
}

case class ConfiguredProxyConfig(
  host:           String,
  port:           Int,
  proxyType:      ProxyType,
  authentication: Option[BasicAuthentication],
) extends ProxyConfig {

  private val javaProxyType = proxyType.javaProxyType

  def createProxyServer(): Proxy = {
    val proxyAddr = new InetSocketAddress(host, port)
    new Proxy(proxyType.javaProxyType, proxyAddr)
  }

  def createHttpClient(): OkHttpClient = {

    val httpClientBuilder = new OkHttpClient.Builder().connectionPool(
      new ConnectionPool(0, 500, TimeUnit.MILLISECONDS),
    ).proxy(createProxyServer())
    authentication match {
      case Some(auth) if javaProxyType == JavaProxyType.HTTP  => configureHttpProxyAuth(httpClientBuilder, auth)
      case Some(auth) if javaProxyType == JavaProxyType.SOCKS => configureSocksProxyAuth(auth)
      case _                                                  => // nothing
    }
    httpClientBuilder.build()
  }

  private def configureSocksProxyAuth(auth: BasicAuthentication): Unit =
    JavaAuthenticator.setDefault(
      new JavaAuthenticator() {
        override def getPasswordAuthentication: PasswordAuthentication =
          Option.when(getRequestingHost.equalsIgnoreCase(host) && getRequestingPort == port) {
            new PasswordAuthentication(auth.username, auth.password.toCharArray)
          }.orNull
      },
    )

  private def configureHttpProxyAuth(httpClientBuilder: OkHttpClient.Builder, auth: BasicAuthentication) =
    httpClientBuilder.proxyAuthenticator(
      new OkHttpAuthenticator() {
        override def authenticate(route: Route, response: Response): Request = {
          val credential = Credentials.basic(auth.username, auth.password)
          response.request().newBuilder()
            .header("Proxy-Authorization", credential)
            .build()
        }
      },
    )

  override def authorizationHeader(): Option[String] = authentication.map(_.encode())
}
