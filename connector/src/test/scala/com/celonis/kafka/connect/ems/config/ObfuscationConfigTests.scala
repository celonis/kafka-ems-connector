/*
 * Copyright 2022 Celonis SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

class ObfuscationConfigTests extends AnyFunSuite with Matchers {
  test(s"return no obfuscation if $OBFUSCATED_FIELDS_KEY is not provided") {
    ObfuscationConfig.extract(Map.empty) shouldBe Right(None)
    ObfuscationConfig.extract(Map("a" -> "b", "b" -> 1)) shouldBe Right(None)
    ObfuscationConfig.extract(Map("a" -> "b", OBFUSCATED_FIELDS_KEY + ".ext" -> 1)) shouldBe Right(None)
  }

  test(s"return an error if $OBFUSCATED_FIELDS_KEY contains invalid field names") {
    ObfuscationConfig.extract(Map(OBFUSCATED_FIELDS_KEY -> "")) shouldBe Left(
      s"Invalid [$OBFUSCATED_FIELDS_KEY]. Empty list of fields has been provided.",
    )
  }

  test(s"return an error if $OBFUSCATED_FIELDS_KEY contains invalid field value") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> 1, OBFUSCATION_TYPE_KEY -> "sha1"),
    ) shouldBe Left(
      s"Invalid [$OBFUSCATED_FIELDS_KEY]. Invalid value provided. Expected a list of obfuscated fields but found:java.lang.Integer.",
    )
  }

  test("return an error if the obfuscation type is incorrect") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "whaaa"),
    ) shouldBe Left(s"Invalid [$OBFUSCATION_TYPE_KEY]. Expected obfuscation methods are: *, sha1 or sha512.")
  }

  test(s"return an error if $OBFUSCATION_TYPE_KEY is missing") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z "),
    ) shouldBe Left(
      s"Invalid [$OBFUSCATION_TYPE_KEY]. Obfuscation method is required.",
    )
  }

  test(s"return an error if $OBFUSCATION_TYPE_KEY is empty") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> ""),
    ) shouldBe Left(
      s"Invalid [$OBFUSCATION_TYPE_KEY]. Obfuscation method is required.",
    )
  }

  test(s"return an error if $SHA512_SALT_KEY is missing") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ", OBFUSCATION_TYPE_KEY -> "sHa512"),
    ) shouldBe Left(
      s"Invalid [$SHA512_SALT_KEY]. Required field. A salt or random salt must be configured.",
    )
  }

  test(s"return an error if $SHA512_SALT_KEY is empty") {
    ObfuscationConfig.extract(
      Map(OBFUSCATED_FIELDS_KEY -> "abc, foo.boo.moo, a.x.y.z ",
          OBFUSCATION_TYPE_KEY  -> "sHa512",
          SHA512_SALT_KEY       -> "",
      ),
    ) shouldBe Left(
      s"Invalid [$SHA512_SALT_KEY]. Required field. A salt or random salt must be configured.",
    )
  }

  test("return the obfuscation keys") {
    ObfuscationConfig.extract(
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

    ObfuscationConfig.extract(
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

    ObfuscationConfig.extract(
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

    val actual = ObfuscationConfig.extract(
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

    ObfuscationConfig.extract(
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
