package com.celonis.kafka.connect.transform

import scala.jdk.CollectionConverters._

class FlattenConfigTest extends org.scalatest.funsuite.AnyFunSuite {

  test("loads default config from an empty map") {
    val result = FlattenConfig.apply(Map.empty[String, String].asJava)
    assertResult(FlattenConfig())(result)
  }

  test("parses valid CaseTransform values") {
    CaseTransform.values.foreach { caseTransform =>
      val props = Map(
        FlattenConfig.TransformCaseKey -> caseTransform.entryName,
      )
      assertResult(FlattenConfig().copy(keyCaseTransformation = Some(caseTransform))) {
        FlattenConfig.apply(props.asJava)
      }
    }
  }

  test("ignore invalid CaseTransform values") {
    val props = Map(
      FlattenConfig.TransformCaseKey -> "invalid-value",
    )
    assertResult(FlattenConfig.apply()) {
      FlattenConfig.apply(props.asJava)
    }
  }
}
