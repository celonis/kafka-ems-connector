/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.syntax.either._
import enumeratum._
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.asynchttpclient.Realm
import org.asynchttpclient.proxy.ProxyServer
import org.asynchttpclient.proxy.{ ProxyType => AsyncProxyType }

sealed abstract class ProxyType(val proxyType: AsyncProxyType) extends EnumEntry

object ProxyType extends Enum[ProxyType] {

  val values = findValues

  case object Http   extends ProxyType(AsyncProxyType.HTTP)
  case object Socks4 extends ProxyType(AsyncProxyType.SOCKS_V4)
  case object Socks5 extends ProxyType(AsyncProxyType.SOCKS_V5)

}

sealed trait ProxyConfig {
  def createHttpClient(): AsyncHttpClient
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
  override def createHttpClient(): AsyncHttpClient = new DefaultAsyncHttpClient()
}

case class ConfiguredProxyConfig(
  host:           String,
  port:           Int,
  proxyType:      ProxyType,
  authentication: Option[BasicAuthentication],
) extends ProxyConfig {

  def createProxyServer(): ProxyServer = {
    val realmMaybe: Option[Realm] = authentication.map(_.createRealm())
    new ProxyServer.Builder(host, port)
      .setRealm(realmMaybe.orNull)
      .setProxyType(proxyType.proxyType)
      .build()
  }

  def createHttpClient(): AsyncHttpClient = {
    val asyncHttpClientConfig =
      new DefaultAsyncHttpClientConfig.Builder().setProxyServer(createProxyServer()).build()
    new DefaultAsyncHttpClient(asyncHttpClientConfig)
  }
}

case class BasicAuthentication(
  username: String,
  password: String,
) {
  def createRealm(): Realm =
    new Realm.Builder(username, password)
      .setUsePreemptiveAuth(true)
      .setScheme(Realm.AuthScheme.BASIC)
      .build()
}
