/*
 * Copyright 2017-2021 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.utils

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.connector.ConnectRecord

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.immutable.Seq
import scala.collection.mutable

case class ProgressCounter(periodMillis: Int = 60000) extends StrictLogging {
  private val startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
  private var timestamp: Long = 0
  private val counter = mutable.Map.empty[String, Long]

  def update[T <: ConnectRecord[T]](records: Seq[ConnectRecord[T]]): Unit = {
    val newTimestamp = System.currentTimeMillis()

    records.foreach(r => counter.put(r.topic(), counter.getOrElse(r.topic(), 0L) + 1L))

    if ((newTimestamp - timestamp) >= periodMillis && records.nonEmpty) {
      counter.foreach({ case (k, v) => logger.info(s"Delivered [$v] records for [$k] since $startTime") })
      counter.empty
      timestamp = newTimestamp
    }
  }

  def empty(): Unit = counter.clear()
}
