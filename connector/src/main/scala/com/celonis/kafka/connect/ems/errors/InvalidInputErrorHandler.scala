/*
 * Copyright 2024 Celonis SE
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

package com.celonis.kafka.connect.ems.errors

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord

/** Error policies work at the batch level, while this handler works at the record level. It works only for
  * InvalidInputExceptions, that are errors due to defects of single records.
  */
final class InvalidInputErrorHandler(
  continueOnInvalidInput: Boolean,
  errantRecordReporter:   Option[ErrantRecordReporter],
) extends StrictLogging {
  def handle(record: SinkRecord, error: Throwable): Unit = error match {
    case _: InvalidInputException if continueOnInvalidInput =>
      logger.warn("Error policy is set to CONTINUE on InvalidInput", error)
      errantRecordReporter.foreach(errantRecordReporter => errantRecordReporter.report(record, error))
    case _ => throw error
  }
}
