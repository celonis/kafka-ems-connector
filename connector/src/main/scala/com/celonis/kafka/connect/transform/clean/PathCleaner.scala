package com.celonis.kafka.connect.transform.clean

import com.celonis.kafka.connect.transform.FlattenConfig

import scala.collection.mutable

object PathCleaner {

  private def retainAfter(str: String)(implicit config: FlattenConfig): String =
    config.keyRetainAfter.map(find => str.substring(str.lastIndexOf(find) + 1)).getOrElse(str)

  private def retainBefore(str: String)(implicit config: FlattenConfig): String =
    config.keyRetainBefore.map(find => str.substring(str.indexOf(find) - 1)).getOrElse(str)

  def cleanPath(path: Seq[String])(implicit config: FlattenConfig): Seq[String] =
    deduplicate(path
      .filterNot(config.keyDiscard.contains)
      .map(retainBefore)
      .map(retainAfter)
      .map(changeCase))

  private def changeCase(str: String)(implicit config: FlattenConfig): String =
    config.keyCaseTransformation.map(transform => transform.transform(str)).getOrElse(str)

  private def deduplicate(path: Seq[String])(implicit config: FlattenConfig): Seq[String] =
    if (config.deduplicateKeys) {
      mutable.LinkedHashSet.from(path).toSeq
    } else {
      path
    }

}
