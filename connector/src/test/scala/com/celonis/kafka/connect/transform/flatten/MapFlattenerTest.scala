package com.celonis.kafka.connect.transform.flatten

import com.celonis.kafka.connect.transform.FlattenConfig
import com.celonis.kafka.connect.transform.LeafNode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MapFlattenerTest extends AnyFunSuite with Matchers {
  implicit val config = FlattenConfig()

  test("should handle empty maps") {
    MapFlattener.flatten(Seq("path"), Map.empty) should be(Seq())
  }

  test("should not be throwing NPEs") {
    MapFlattener.flatten(Seq("path"), null) should be(Seq.empty[LeafNode])
  }

  test("should handle maps with empty elements") {
    MapFlattener.flatten(Seq("path"), Map("macaroni" -> null)) should be(Seq(LeafNode(Seq("path", "macaroni"), null)))
  }

}
