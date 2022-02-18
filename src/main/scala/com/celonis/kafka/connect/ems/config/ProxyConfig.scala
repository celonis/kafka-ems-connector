/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.asynchttpclient.Realm
import org.asynchttpclient.proxy.ProxyServer
import org.asynchttpclient.proxy.{ ProxyType => AsyncProxyType }
import enumeratum._

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
