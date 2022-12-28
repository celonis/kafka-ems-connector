package com.celonis.kafka.connect.transform.clean

import com.celonis.kafka.connect.transform.FlattenerConfig

import scala.collection.mutable

object PathCleaner {

  private def retainAfter(str: String)(implicit config: FlattenerConfig): String =
    config.keyRetainAfter.map(find => str.substring(str.lastIndexOf(find) + 1)).getOrElse(str)

  private def retainBefore(str: String)(implicit config: FlattenerConfig): String =
    config.keyRetainBefore.map(find => str.substring(str.indexOf(find) - 1)).getOrElse(str)

  def cleanPath(path: Seq[String])(implicit config: FlattenerConfig): Seq[String] =
    deduplicate(
      path
        .filterNot(config.keyDiscard.contains)
        //^ TODO: is this correctly handled? shouldn't discarding a key be handled when building the struct?
        .map(retainBefore)
        .map(retainAfter)
        .map(changeCase),
    )

  private def changeCase(str: String)(implicit config: FlattenerConfig): String =
    config.keyCaseTransformation.map(transform => transform.transform(str)).getOrElse(str)

  //TODO: why is this needed? How come we end up with duplicate paths?
  private def deduplicate(path: Seq[String])(implicit config: FlattenerConfig): Seq[String] =
    if (config.deduplicateKeys) {
      mutable.LinkedHashSet.from(path).toSeq
    } else {
      path
    }

}
