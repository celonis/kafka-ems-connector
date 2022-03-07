/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import java.io.FileInputStream
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory

object SSLConfigContext {
  def apply(config: SSLConfig): SSLContext =
    getSSLContext(config)

  /**
    * Get a SSL Connect for a given set of credentials
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return a SSLContext
    */
  def getSSLContext(config: SSLConfig): SSLContext = {
    val useClientCertAuth = config.useClientCert

    //is client certification authentication set
    val keyManagers: Array[KeyManager] = if (useClientCertAuth) {
      getKeyManagers(config)
    } else {
      Array[KeyManager]()
    }

    val ctx: SSLContext = SSLContext.getInstance("SSL")
    val trustManagers = getTrustManagers(config)
    ctx.init(keyManagers, trustManagers, new SecureRandom())
    ctx
  }

  /**
    * Get an array of Trust Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of TrustManagers
    */
  def getTrustManagers(config: SSLConfig): Array[TrustManager] = {
    val tsf = new FileInputStream(config.trustStorePath)
    val ts  = KeyStore.getInstance(config.trustStoreType)
    ts.load(tsf, config.trustStorePass.toCharArray)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    tmf.getTrustManagers
  }

  /**
    * Get an array of Key Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of KeyManagers
    */
  def getKeyManagers(config: SSLConfig): Array[KeyManager] = {
    require(config.keyStorePath.nonEmpty, "Key store path is not set!")
    require(config.keyStorePass.nonEmpty, "Key store password is not set!")
    val ksf = new FileInputStream(config.keyStorePath.get)
    val ks  = KeyStore.getInstance(config.keyStoreType)
    ks.load(ksf, config.keyStorePass.get.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, config.keyStorePass.get.toCharArray)
    kmf.getKeyManagers
  }

}

/**
  * Class for holding key and truststore settings
  */
case class SSLConfig(
  trustStorePath: String,
  trustStorePass: String,
  keyStorePath:   Option[String],
  keyStorePass:   Option[String],
  useClientCert:  Boolean = false,
  keyStoreType:   String  = "JKS",
  trustStoreType: String  = "JKS",
)
