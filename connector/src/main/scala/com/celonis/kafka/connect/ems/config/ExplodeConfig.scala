/*
 * Copyright 2023 Celonis SE
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
  import EmsSinkConfigConstants._

  def apply(config: Option[String]): ExplodeConfig =
    config.flatMap(ExplodeConfig.withNameInsensitiveOption).getOrElse(None)

  val values: IndexedSeq[ExplodeConfig] = findValues

  case object None extends ExplodeConfig(() => new NoOpExploder())
  case object List extends ExplodeConfig(() => new ListExploder())

  def extractExplode(props: Map[String, _]): ExplodeConfig =
    ExplodeConfig(PropertiesHelper.getString(props, EXPLODE_MODE_KEY))
}
