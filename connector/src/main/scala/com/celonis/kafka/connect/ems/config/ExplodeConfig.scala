/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.data.NonEmptySeq
import com.celonis.kafka.connect.ems.storage.formats.Exploder
import com.celonis.kafka.connect.ems.storage.formats.ListExploder
import com.celonis.kafka.connect.ems.storage.formats.NoOpExploder
import enumeratum._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

sealed abstract class ExplodeConfig(createExploderFn: () => Exploder) extends EnumEntry {
  def toExplodeFn: GenericRecord => NonEmptySeq[GenericRecord] = createExploderFn().explode

  def explodeSchema(schema: Schema): Schema = createExploderFn().explodeSchema(schema)
}

object ExplodeConfig extends Enum[ExplodeConfig] {

  def apply(config: Option[String]): ExplodeConfig =
    config.flatMap(ExplodeConfig.withNameInsensitiveOption).getOrElse(None)

  val values: IndexedSeq[ExplodeConfig] = findValues

  case object None extends ExplodeConfig(() => new NoOpExploder())
  case object List extends ExplodeConfig(() => new ListExploder())

}
