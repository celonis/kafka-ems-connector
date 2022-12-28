package com.celonis.kafka.connect.transform

import scala.jdk.CollectionConverters._

class FlattenerConfigTest extends org.scalatest.funsuite.AnyFunSuite {

  test("loads default config from an empty map") {
    val result = FlattenerConfig.apply(Map.empty[String, String].asJava)
    assertResult(FlattenerConfig())(result)
  }

  test("parses valid CaseTransform values") {
    CaseTransform.values.foreach { caseTransform =>
      val props = Map(
        FlattenerConfig.TransformCaseKey -> caseTransform.entryName,
      )
      assertResult(FlattenerConfig().copy(keyCaseTransformation = Some(caseTransform))) {
        FlattenerConfig.apply(props.asJava)
      }
    }
  }

  test("ignore invalid CaseTransform values") {
    val props = Map(
      FlattenerConfig.TransformCaseKey -> "invalid-value",
    )
    assertResult(FlattenerConfig.apply()) {
      FlattenerConfig.apply(props.asJava)
    }
  }
}
