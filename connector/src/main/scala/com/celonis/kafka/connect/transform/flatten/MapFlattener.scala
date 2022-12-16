/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.LeafNode
import com.celonis.kafka.connect.transform.Node
import org.apache.kafka.connect.data.Struct

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala

object MapFlattener {

  def flatten(path: Seq[String], possiblyNullMap: Map[_, _])(implicit config: FlattenConfig): Seq[Node] =
    if (config.discardCollections)
      Nil
    else
      Option(possiblyNullMap).fold(Seq.empty[Node])(map =>
        map.view.flatMap {
          case (k: String, v: Byte) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Short) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Int) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Long) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Float) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Double) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Boolean) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: String) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: Array[Byte]) => Seq(LeafNode(path :+ k, v))
          case (k: String, v: util.Map[_, _]) => MapFlattener.flatten(path :+ k, v.asScala.toMap)
          case (k: String, v: util.List[_]) => ArrayFlattener.flatten(path :+ k, v, Option.empty)
          case (k: String, v: Struct) => StructFlattener.flatten(path :+ k, v)
          case (k: String, null) => Seq(LeafNode(path :+ k, null))
          case (_, null) => Seq()
        }
          .toSeq,
      )

}
