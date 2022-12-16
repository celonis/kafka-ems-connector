/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.transform

import org.apache.kafka.connect.data.Schema

trait Node {
  def path:  Seq[String]
  def value: Any
}

case class LeafNode(
  path:  Seq[String],
  value: Any,
) extends Node

case class SchemaLeafNode(
  path:  Seq[String],
  sType: Schema.Type,
  value: Schema,
) extends Node
