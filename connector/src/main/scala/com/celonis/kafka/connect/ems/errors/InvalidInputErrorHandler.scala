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
