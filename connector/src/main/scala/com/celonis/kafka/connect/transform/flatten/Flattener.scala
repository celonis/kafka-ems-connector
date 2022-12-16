/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import cats.implicits._
import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.LeafNode
import com.celonis.kafka.connect.transform.Node
import com.celonis.kafka.connect.transform.clean.PathCleaner.cleanPath
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Date => KafkaDate }
import org.apache.kafka.connect.data.{ Decimal => KafkaDecimal }
import org.apache.kafka.connect.data.{ Time => KafkaTime }
import org.apache.kafka.connect.data.{ Timestamp => KafkaTimestamp }

import java.util
import scala.jdk.CollectionConverters._

object Flattener extends LazyLogging {

  /**
    * Flattens a Kafka Connect record value
    *
    * @param value A kafka connect record value
    * @param maybeFlattenedSchema an optional, flattened schema (i.e. the representation of target record shape).
    * @param config
    * @return
    */
  def flatten(
    value:                AnyRef,
    maybeFlattenedSchema: Option[Schema],
  )(
    implicit
    config: FlattenConfig,
  ): Either[String, AnyRef] =
    for {
      newValue    <- transformToNodeSeq(value)
      intermediate = compressAndClean(newValue)
      transformTo <- value match {
        case _: Struct =>
          rebuildStruct(maybeFlattenedSchema, intermediate)
        case _ =>
          intermediate.toMap.asJava.asRight
      }
    } yield transformTo

  private[flatten] def convertLogicalType(s: Struct, k: String, logicalTypeValue: Any, field: Field): Any =
    field.schema().name() match {
      case KafkaTimestamp.LOGICAL_NAME => logicalTypeValue
      case KafkaDecimal.LOGICAL_NAME   => logicalTypeValue
      case KafkaDate.LOGICAL_NAME      => logicalTypeValue
      case KafkaTime.LOGICAL_NAME      => logicalTypeValue
      case _ =>
        throw new IllegalArgumentException(
          s"Unhandled logical type n:${field.schema().name()}, k:$k v:$logicalTypeValue",
        )
    }

  private def rebuildStruct(
    maybeSchema:  Option[Schema],
    intermediate: Seq[(String, Any)],
  )(
    implicit
    config: FlattenConfig,
  ): Either[String, Struct] =
    maybeSchema
      .map {
        schema0: Schema =>
//          val schema = flattenSchema(schema0)
          val _ = config
          intermediate.foldLeft(new Struct(schema0)) {
            case (s: Struct, (k: String, v: Any)) => s.put(k, v)
            case (s: Struct, (k: String, null)) => s.put(k, null)
          }.asRight

      }
      .getOrElse("No schema".asLeft)

  private def compressAndClean(newValue: Seq[Node])(implicit config: FlattenConfig) =
    newValue.filterNot(_.value == null && config.filterNulls)
      .map((ln: Node) => compress(cleanPath(ln.path)).mkString("_") -> ln.value)

  private def transformToNodeSeq(value: AnyRef)(implicit config: FlattenConfig): Either[String, Seq[Node]] =
    value match {
      case struct: Struct         => StructFlattener.flatten(Seq.empty, struct).asRight
      case list:   util.List[_]   => ArrayFlattener.flatten(Seq.empty, list, Option.empty).asRight
      case map:    util.Map[_, _] => MapFlattener.flatten(Seq.empty, map.asScala.toMap).asRight
      case Byte | Short | Int | Long | Float | Double | Boolean | _: String | _: Array[Byte] =>
        // doesn't require flattening
        Seq(LeafNode(Seq.empty, value)).asRight
      case other => s"Unaddressed condition: $other".asLeft
    }

  private def compress(seqToCompress: Seq[String]): Seq[String] = seqToCompress.foldLeft(Seq[String]()) {
    case (Seq(), e)              => Seq(e)
    case (ls, e) if ls.last == e => ls
    case (ls, e)                 => ls ++ Seq(e)
  }
}
