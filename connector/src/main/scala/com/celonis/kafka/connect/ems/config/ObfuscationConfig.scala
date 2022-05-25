package com.celonis.kafka.connect.ems.config

import cats.data.NonEmptyList
import cats.syntax.all._
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants._
import com.celonis.kafka.connect.ems.config.PropertiesHelper.error
import com.celonis.kafka.connect.ems.model.DataObfuscation
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA1
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithRandomSalt
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithSalt

import java.nio.charset.StandardCharsets
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class ObfuscationConfig(obfuscation: DataObfuscation, fields: NonEmptyList[ObfuscatedField])

object ObfuscationConfig {
  def extract(props: Map[String, _]): Either[String, Option[ObfuscationConfig]] =
    props.get(OBFUSCATED_FIELDS_KEY) match {
      case Some(value: String) =>
        val fields = value.split(',').map(_.trim).filter(_.nonEmpty).distinct
        if (fields.isEmpty) error(OBFUSCATED_FIELDS_KEY, "Empty list of fields has been provided.")
        else {
          val invalidFields = fields.map(f => f -> fields.count(_ == f)).filter(_._2 > 1)
          if (invalidFields.nonEmpty)
            error(
              OBFUSCATED_FIELDS_KEY,
              s"Invalid obfuscation fields. There are overlapping fields:${invalidFields.map(_._1).mkString("m,")}.",
            )
          else
            ObfuscationConfig.extractObfuscationMethod(props).map { dataObfuscation =>
              ObfuscationConfig(
                dataObfuscation,
                NonEmptyList.fromListUnsafe(
                  fields.map(f => NonEmptyList.fromListUnsafe(f.split('.').toList)).map(ObfuscatedField.apply).toList,
                ),
              ).some
            }

        }
      case Some(other) =>
        Option(other).fold(Option.empty[ObfuscationConfig].asRight[String]) { o =>
          error[Option[ObfuscationConfig]](
            OBFUSCATED_FIELDS_KEY,
            s"Invalid value provided. Expected a list of obfuscated fields but found:${o.getClass.getName}.",
          )
        }
      case None => None.asRight
    }

  private def extractObfuscationMethod(props: Map[String, _]): Either[String, DataObfuscation] =
    PropertiesHelper.getString(props, OBFUSCATION_TYPE_KEY) match {
      case Some(value) =>
        value.toLowerCase() match {
          case "fix"    => FixObfuscation(5, '*').asRight[String]
          case "sha1"   => SHA1.asRight[String]
          case "sha512" => ObfuscationConfig.extractSHA512(props)
          case _        => error(OBFUSCATION_TYPE_KEY, "Expected obfuscation methods are: *, sha1 or sha512.")
        }
      case None => error(OBFUSCATION_TYPE_KEY, "Obfuscation method is required.")
    }

  private def extractSHA512(props: Map[String, _]): Either[String, DataObfuscation] =
    PropertiesHelper.getString(props, SHA512_SALT_KEY) match {
      case Some(value) =>
        Try(SHA512WithSalt(value.getBytes(StandardCharsets.UTF_8))) match {
          case Failure(exception) => error(SHA512_SALT_KEY, s"Invalid salt provided. ${exception.getMessage}")
          case Success(value)     => value.asRight
        }
      case None =>
        PropertiesHelper.getBoolean(props, SHA512_RANDOM_SALT_KEY) match {
          case Some(true) => SHA512WithRandomSalt().asRight
          case _          => error(SHA512_SALT_KEY, "Required field. A salt or random salt must be configured.")
        }
    }
}

case class ObfuscatedField(path: NonEmptyList[String])
