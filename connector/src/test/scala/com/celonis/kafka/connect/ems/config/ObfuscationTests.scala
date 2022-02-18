/*
 * Copyright 2017-2022 Celonis Ltd
 */
package com.celonis.kafka.connect.ems.config

import cats.data.NonEmptyList
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATED_FIELDS_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.OBFUSCATION_TYPE_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_RANDOM_SALT_KEY
import com.celonis.kafka.connect.ems.config.EmsSinkConfigConstants.SHA512_SALT_KEY
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA1
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithSalt
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.model.DataObfuscation.SHA512WithRandomSalt
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class ObfuscationTests extends AnyFunSuite with Matchers {
  test(s"return no obfuscation if $OBFUSCATED_FIELDS_KEY is not provided") {
    EmsSinkConfig.extractObfuscation(Map.empty) shouldBe Right(None)
    EmsSinkConfig.extractObfuscation(Map("a" -> "b", "b" -> 1)) shouldBe Right(None)
    EmsSinkConfig.extractObfuscation(Map("a" -> "b", OBFUSCATED_FIELDS_KEY + ".ext" -> 1)) shouldBe Right(None)
  }

  test(s"return an error if $OBFUSCATED_FIELDS_KEY contains invalid field names") {
    EmsSinkConfig.extractObfuscation(Map(OBFUSCATED_FIELDS_KEY -> "")) shouldBe Left(
      s"Invalid [$OBFUSCATED_FIELDS_KEY]. Empty list of fields has been provided.",
    )
  }

  test("return an error if the obfuscation type is incorrect") {
    EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "whaaa"),
    ) shouldBe Left(s"Invalid [$OBFUSCATION_TYPE_KEY]. Expected obfuscation methods are: *, sha1 or sha512.")
  }

  test("return the obfuscation keys") {
    EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "fix"),
    ) shouldBe Right(
      Some(
        ObfuscationConfig(
          FixObfuscation(5, '*'),
          NonEmptyList.fromListUnsafe(
            List(
              ObfuscatedField(NonEmptyList.of("abc")),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("foo", "boo", "moo"))),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "x", "y", "z"))),
            ),
          ),
        ),
      ),
    )

    EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, ,foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "fix"),
    ) shouldBe Right(
      Some(
        ObfuscationConfig(
          FixObfuscation(5, '*'),
          NonEmptyList.fromListUnsafe(
            List(
              ObfuscatedField(NonEmptyList.of("abc")),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("foo", "boo", "moo"))),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "x", "y", "z"))),
            ),
          ),
        ),
      ),
    )

    EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "shA1"),
    ) shouldBe Right(
      Some(
        ObfuscationConfig(
          SHA1,
          NonEmptyList.fromListUnsafe(
            List(
              ObfuscatedField(NonEmptyList.of("abc")),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("foo", "boo", "moo"))),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "x", "y", "z"))),
            ),
          ),
        ),
      ),
    )

    val actual = EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ",
          OBFUSCATION_TYPE_KEY  -> "shA512",
          SHA512_SALT_KEY       -> "jack norris",
      ),
    ).getOrElse(fail("should not fail")).get

    actual.fields shouldBe NonEmptyList.fromListUnsafe(
      List(
        ObfuscatedField(NonEmptyList.of("abc")),
        ObfuscatedField(NonEmptyList.fromListUnsafe(List("foo", "boo", "moo"))),
        ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "x", "y", "z"))),
      ),
    )
    actual.obfuscation match {
      case SHA512WithSalt(salt) => salt shouldBe "jack norris".getBytes(StandardCharsets.UTF_8)
      case _                    => fail("should be SHA512")
    }

    EmsSinkConfig.extractObfuscation(
      Map(OBFUSCATED_FIELDS_KEY  -> "abc, foo.boo.moo, a.x.y.z ",
          OBFUSCATION_TYPE_KEY   -> "sHa512",
          SHA512_RANDOM_SALT_KEY -> "true",
      ),
    ) shouldBe Right(
      Some(
        ObfuscationConfig(
          SHA512WithRandomSalt(),
          NonEmptyList.fromListUnsafe(
            List(
              ObfuscatedField(NonEmptyList.of("abc")),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("foo", "boo", "moo"))),
              ObfuscatedField(NonEmptyList.fromListUnsafe(List("a", "x", "y", "z"))),
            ),
          ),
        ),
      ),
    )
  }
}
