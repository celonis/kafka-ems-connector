package com.celonis.kafka.connect.transform

import enumeratum.{Enum, EnumEntry}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import java.util
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}
import scala.util.{Failure, Success, Try}

sealed trait CaseTransform extends EnumEntry {
  def transform(in: String): String
}

object CaseTransform extends Enum[CaseTransform] {

  val values = findValues

  case object ToUpperCase extends CaseTransform {

    override def transform(in: String): String = in.toUpperCase

  }
  case object ToLowerCase extends CaseTransform {
    override def transform(in: String): String = in.toLowerCase
  }
  case object LowerCaseFirst extends CaseTransform {
    override def transform(in: String): String =
      Try(in.charAt(0)) match {
        case Failure(_)     => in
        case Success(value) => s"${Character.toLowerCase(value)}${in.substring(1)}"
      }
  }
  case object UpperCaseFirst extends CaseTransform {
    override def transform(in: String): String =
      Try(in.charAt(0)) match {
        case Failure(_)     => in
        case Success(value) => s"${Character.toUpperCase(value)}${in.substring(1)}"
      }
  }

}

case class FlattenConfig(
  keyDiscard:            Set[String]           = Set.empty[String],
  keyRetainAfter:        Option[String]        = Option.empty,
  keyRetainBefore:       Option[String]        = Option.empty,
  keyCaseTransformation: Option[CaseTransform] = Option.empty,
  deduplicateKeys:       Boolean               = false,
  filterNulls:           Boolean               = true,
  discardCollections:    Boolean               = false,
)

object FlattenConfig {
  final val DiscardKey           = "discard"
  final val RetainAfterKey       = "retainAfter"
  final val RetainBeforeKey      = "retainBefore"
  final val TransformCaseKey     = "transformCase"
  final val DeduplicateInPathKey = "deduplicateInPath"
  final val FilterNulls          = "filterNulls"
  final val DiscardCollections   = "discardCollections"

  def configDef = new ConfigDef()
    .define(
      DiscardKey,
      Type.LIST,
      List.empty[String].asJava,
      Importance.LOW,
      "List of complete strings to remove if found in the path of the field name",
    )
    .define(
      RetainAfterKey,
      Type.STRING,
      null,
      Importance.LOW,
      "Configure to retain all text after a given string if found in the key name",
    )
    .define(
      RetainBeforeKey,
      Type.STRING,
      null,
      Importance.LOW,
      "Configure to retain all text before a given string if found in the key name",
    )
    .define(
      TransformCaseKey,
      Type.STRING,
      null,
      Importance.LOW,
      "Perform a case transformation on the key value. The options are 'ToUpperCase', 'ToLowerCase', 'UpperCaseFirst' and 'LowerCaseFirst'",
    )
    .define(
      DeduplicateInPathKey,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Duplicate keys in path",
    )
    .define(
      FilterNulls,
      Type.BOOLEAN,
      true,
      Importance.LOW,
      "Filter null values",
    )
    .define(
      DiscardCollections,
      Type.BOOLEAN,
      false,
      Importance.MEDIUM,
      "Discard array and map fields at any level of depth",
    )

  def apply(confMap: util.Map[String, _]): FlattenConfig = {
    val abstractConfig     = new AbstractConfig(configDef, confMap)
    val discardKey         = abstractConfig.getList(DiscardKey)
    val retainAfter        = Option(abstractConfig.getString(RetainAfterKey))
    val retainBefore       = Option(abstractConfig.getString(RetainBeforeKey))
    val deduplicate        = abstractConfig.getBoolean(DeduplicateInPathKey)
    val filterNulls        = abstractConfig.getBoolean(FilterNulls)
    val discardCollections = abstractConfig.getBoolean(DiscardCollections)

    val caseTransform =
      Option(abstractConfig.getString(TransformCaseKey)).flatMap(CaseTransform.withNameInsensitiveOption)

    FlattenConfig(
      discardKey.asScala.toSet,
      retainAfter,
      retainBefore,
      caseTransform,
      deduplicate,
      filterNulls,
      discardCollections,
    )
  }
}
