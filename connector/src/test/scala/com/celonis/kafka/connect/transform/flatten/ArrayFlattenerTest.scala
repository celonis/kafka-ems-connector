package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.LeafNode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.SeqHasAsJava

class ArrayFlattenerTest extends AnyFunSuite with Matchers {
  implicit val config = FlattenConfig()

  test("should handle empty lists") {
    ArrayFlattener.flatten(Seq("path"), List.empty[String].asJava, Option.empty) should be(Seq(LeafNode(Seq("path"),
                                                                                                        "[]",
    )))
  }

  test("should not be throwing NPEs") {
    ArrayFlattener.flatten(Seq("path"), null, Option.empty) should be(Seq.empty[LeafNode])
  }

}
