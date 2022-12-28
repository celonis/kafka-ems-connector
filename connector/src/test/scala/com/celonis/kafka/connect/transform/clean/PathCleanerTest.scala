package com.celonis.kafka.connect.transform.clean

import cats.implicits.catsSyntaxOptionId
import com.celonis.kafka.connect.transform.CaseTransform.LowerCaseFirst
import com.celonis.kafka.connect.transform.CaseTransform
import com.celonis.kafka.connect.transform.FlattenerConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PathCleanerTest extends AnyFunSuite with Matchers {

  test("should pass through path unchanged if not requiring changes") {
    implicit val config = FlattenerConfig(Set("notfound"))
    PathCleaner.cleanPath(Seq("", "my", "great", "path")) should be(Seq("", "my", "great", "path"))
  }

  test("rohan test") {
    implicit val config = FlattenerConfig(Set("value"))
    PathCleaner.cleanPath(
      Seq(
        "value",
        "Order",
        "ChargeTransactionDetails",
        "ChargeTransactionDetail",
        "CreditCardTransactions",
        "CreditCardTransaction",
        "CreditCardTransactionKey",
      ),
    ) should be(
      Seq(
        "Order",
        "ChargeTransactionDetails",
        "ChargeTransactionDetail",
        "CreditCardTransactions",
        "CreditCardTransaction",
        "CreditCardTransactionKey",
      ),
    )
  }

  test("should pass through path camelCase if capitalised starts") {
    implicit val config = FlattenerConfig(keyCaseTransformation = LowerCaseFirst.some)
    PathCleaner.cleanPath(Seq("Path", "OfTotal", "Doom")) should be(Seq("path", "ofTotal", "doom"))
  }

  // we could expose this as an option to either include complete path or only keep the part after the dots
  test("should retain only parts of the path after dots") {
    implicit val config = FlattenerConfig(keyRetainAfter = ".".some)
    PathCleaner.cleanPath(
      Seq("com.fully.qualified.domain.chips", "are", "tasty"),
    ) should be(Seq("chips", "are", "tasty"))
  }

  test("should remove keys we don't want to keep") {
    implicit val config = FlattenerConfig(Set("", "string", "array"))

    PathCleaner.cleanPath(Seq("", "beans", "string", "are", "array", "tasty")) should be(Seq("beans", "are", "tasty"))
  }

  test("should deduplicate after case changes") {
    implicit val config = FlattenerConfig(
      keyRetainAfter        = ".".some,
      keyCaseTransformation = CaseTransform.ToLowerCase.some,
      deduplicateKeys       = true,
    )

    PathCleaner.cleanPath(Seq("baked.beans", "tomatoes", "runner.beans", "GREEN.BEANS")) should be(Seq("beans",
                                                                                                       "tomatoes",
    ))
  }
}
