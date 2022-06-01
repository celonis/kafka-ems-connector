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

package com.celonis.kafka.connect.ems.obfuscation

import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import cats.syntax.either._
import com.celonis.kafka.connect.ems.config.ObfuscatedField
import com.celonis.kafka.connect.ems.config.ObfuscationConfig
import com.celonis.kafka.connect.ems.model.DataObfuscation
import org.apache.avro.Schema
import org.apache.avro.generic.GenericArray
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.jdk.CollectionConverters._

object ObfuscationUtils {

  implicit class GenericRecordObfuscation(val record: GenericRecord) extends AnyVal {
    def obfuscate(config: ObfuscationConfig): Either[String, GenericRecord] =
      obfuscate(config.fields, config.obfuscation, Vector.empty)

    def obfuscate(
      fields:      NonEmptyList[ObfuscatedField],
      obfuscation: DataObfuscation,
      path:        Vector[String],
    ): Either[String, GenericRecord] =
      record.getSchema.getFields.asScala.toList.zipWithIndex.traverse {
        case (field, i) =>
          Option(record.get(i)) match {
            case Some(value) =>
              val matchedFilters = fields.filter(_.path.head == field.name())
              matchedFilters match {
                case ::(head, next) =>
                  value match {
                    case s: Utf8 =>
                      if (next.nonEmpty)
                        s"Multiple obfuscations paths provided. ${(path :+ head).mkString(".")} resolves to string.".asLeft
                      else {
                        if (head.path.tail.nonEmpty) {
                          s"Invalid obfuscation path:${(path ++ head.path.toList).mkString(".")}. ${(path :+ head.path.head).mkString(".")} resolves to String.".asLeft
                        } else (i -> new Utf8(obfuscation.obfuscate(s.toString))).asRight[String]
                      }
                    case s: String =>
                      if (next.nonEmpty)
                        s"Multiple obfuscations paths provided. ${(path :+ head).mkString(".")} resolves to string.".asLeft
                      else {
                        if (head.path.tail.nonEmpty) {
                          s"Invalid obfuscation path:${(path ++ head.path.toList).mkString(".")}. ${(path :+ head.path.head).mkString(".")} resolves to String.".asLeft
                        } else (i -> obfuscation.obfuscate(s)).asRight[String]
                      }
                    case r: GenericRecord =>
                      val invalidFilters = matchedFilters.collect { case f if f.path.tail.isEmpty => f }
                      if (invalidFilters.nonEmpty) {
                        s"Invalid obfuscation path:${invalidFilters.map(f => path ++ f.path.toList).map(
                          _.mkString("."),
                        ).mkString(",")}. Path: ${(path :+ field.name()).mkString(".")} resolves to object.".asLeft
                      } else {
                        r.obfuscate(
                          NonEmptyList.fromListUnsafe(matchedFilters.map(f =>
                            ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail)),
                          )),
                          obfuscation,
                          path :+ field.name(),
                        ).map(i -> _)
                      }
                    case a: GenericArray[_] =>
                      a.obfuscate(
                        NonEmptyList.fromListUnsafe(matchedFilters.map(f =>
                          ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail)),
                        )),
                        obfuscation,
                        path :+ field.name(),
                      ).map(i -> _)

                    case jc: java.util.Collection[_] =>
                      val arrayE = if (field.schema().isNullable) {
                        field.schema().getTypes.asScala.find(_.getType != Schema.Type.NULL) match {
                          case Some(value) => new GenericData.Array(value, jc).asRight
                          case None        => s"Invalid state. Received an array for the path: ${path :+ field.name()}".asLeft
                        }
                      } else new GenericData.Array(field.schema(), jc).asRight
                      arrayE.flatMap(
                        _.obfuscate(
                          NonEmptyList.fromListUnsafe(matchedFilters.map(f =>
                            ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail)),
                          )),
                          obfuscation,
                          path :+ field.name(),
                        ).map(i -> _),
                      )

                    case _ =>
                      s"Invalid obfuscation path: ${(path ++ head.path.toList).mkString(
                        ".",
                      )}. Path: ${(path :+ field.name()).mkString(".")} resolves to ${field.schema().getType} which is not allowed.".asLeft
                  }
                case Nil => (i -> value).asRight[String]
              }
            case None => (i -> null).asRight[String]
          }
      }.map { values =>
        val newRecord = new GenericData.Record(record.getSchema)
        values.foreach { case (index, value) => newRecord.put(index, value) }
        newRecord
      }
  }

  implicit class ArrayObfuscation(val array: GenericArray[_]) extends AnyVal {
    def obfuscate(config: ObfuscationConfig): Either[String, GenericData.Array[_]] =
      obfuscate(config.fields, config.obfuscation, Vector.empty)

    def obfuscate(
      fields:      NonEmptyList[ObfuscatedField],
      obfuscation: DataObfuscation,
      path:        Vector[String],
    ): Either[String, GenericData.Array[_]] = {
      val invalidFields = fields.filterNot(_.path.head == "value")
      if (invalidFields.nonEmpty)
        s"Invalid obfuscation path:${(path ++ invalidFields.head.path.toList).mkString(".")}. The value resolves to an array and it requires: value. For example: playersName.value (when playersName is a sequence of strings), players.value.name.".asLeft
      else {
        val lastPath    = fields.filter(_.path.tail.isEmpty)
        val notLastPath = fields.filter(_.path.tail.nonEmpty)
        if (lastPath.nonEmpty && notLastPath.nonEmpty)
          s"Invalid obfuscation path. There are overlapping paths: ${fields.map(f => (path ++ f.path.toList).mkString(".")).toList.mkString(",")}.".asLeft
        else {
          //we have primitives only
          val items = if (lastPath.nonEmpty) {
            array.iterator().asScala.toList.traverse {
              case null => null.asInstanceOf[Any].asRight
              case s: Utf8   => new Utf8(obfuscation.obfuscate(s.toString)).asRight
              case s: String => obfuscation.obfuscate(s).asRight
              case _ =>
                s"Invalid obfuscation path: ${fields.map(f => (path ++ f.path.toList).mkString(".")).toList.mkString(
                  ",",
                )}. Obfuscation resolves array element to ${array.getSchema.getType} which is not allowed.".asLeft[
                  String,
                ]
            }
          } else {
            //it obfuscates further for Struct/Arrays
            array.iterator().asScala.toList.traverse {
              case null => null.asInstanceOf[Any].asRight[String]
              case r: GenericRecord =>
                r.obfuscate(fields.map(f => ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail))),
                            obfuscation,
                            path :+ "value",
                )
              case a: GenericArray[_] =>
                a.obfuscate(fields.map(f => ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail))),
                            obfuscation,
                            path :+ "value",
                )

              case jc: java.util.Collection[_] =>
                val arrayE = if (array.getSchema.getElementType.isNullable) {
                  array.getSchema.getElementType.getTypes.asScala.find(_.getType != Schema.Type.NULL) match {
                    case Some(value) => new GenericData.Array(value, jc).asRight
                    case None        => s"Invalid schema state. Received an array for the path: ${path.mkString(".")}".asLeft
                  }
                } else new GenericData.Array(array.getSchema.getElementType, jc).asRight
                arrayE.flatMap(_.obfuscate(fields.map(f => ObfuscatedField(NonEmptyList.fromListUnsafe(f.path.tail))),
                                           obfuscation,
                                           path :+ "value",
                ))

              case _ =>
                s"Invalid obfuscation path: ${fields.map(f => (path ++ f.path.toList).mkString(".")).toList.mkString(
                  ",",
                )}. Obfuscation resoles array element to ${array.getSchema.getType}.".asLeft[Any]
            }
          }
          items.map(items => new GenericData.Array[Any](array.getSchema, items.asJava))
        }
      }
    }
  }
}
