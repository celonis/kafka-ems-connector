package com.celonis.kafka.connect.ems.config

import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_DOC
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.AUTHORIZATION_KEY
import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.config.PropertiesHelper.nonEmptyPasswordOr

final case class AuthorizationHeader(header: String)

object AuthorizationHeader {
  def extract(props: Map[String, _]): Either[String, AuthorizationHeader] =
    nonEmptyPasswordOr(props, AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      .map(_.trim)
      .flatMap { auth =>
        val filtered = auth.filter(_ != '"')
        if (filtered.startsWith("AppKey") || filtered.startsWith("Bearer")) AuthorizationHeader(filtered).asRight
        else error(AUTHORIZATION_KEY, AUTHORIZATION_DOC)
      }
}
