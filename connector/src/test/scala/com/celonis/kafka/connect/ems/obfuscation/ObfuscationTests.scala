/*
 * Copyright 2023 Celonis SE
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

package com.celonis.kafka.connect.ems.obfuscation

import cats.data.NonEmptyList
import cats.implicits.catsSyntaxOptionId
import com.celonis.kafka.connect.ems.config.ObfuscatedField
import com.celonis.kafka.connect.ems.model.DataObfuscation.FixObfuscation
import com.celonis.kafka.connect.ems.obfuscation.ObfuscationUtils._
import com.celonis.kafka.connect.ems.obfuscation.PlayerType.Goalkeeper
import com.celonis.kafka.connect.ems.obfuscation.PlayerType.Striker
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ObfuscationTests extends AnyFunSuite with Matchers {
  private val obfuscation = FixObfuscation(5, '*')
  test(s"obfuscates ${classOf[Player].getName} name") {
    val player = Player("James ItsAllGood", 11, 89.11, Striker)
    Player.format.to(player).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.one("name"))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) => fail(s"Should not fail. $value")
      case Right(value) =>
        Player.format.from(value) shouldBe player.copy(name = obfuscation.constant)
    }
  }

  test(s"obfuscates ${classOf[Team].getName} player name") {
    val player1 = Player("James ItsAllGood", 11, 89.11, Striker)
    val player2 = Player("Mark Green", 25, 79.33, Goalkeeper)
    val team    = Team("the_best", List(player1, player2))
    Team.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "name")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) => fail(s"Should not fail. $value")
      case Right(value) =>
        Team.format.from(value) shouldBe Team(
          "the_best",
          List(player1.copy(name = obfuscation.constant), player2.copy(name = obfuscation.constant)),
        )
    }
  }

  test(s"handles null players for ${OptionalTeam.getClass.getName}") {
    val team = OptionalTeam("the_best".some, None)
    OptionalTeam.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "name")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) => fail(s"Should not fail. $value")
      case Right(value) =>
        OptionalTeam.format.from(value) shouldBe team
    }
  }

  test(s"obfuscates ${OptionalTeam.getClass.getName} players name") {
    val player1 = OptionalPlayer("James ItsAllGood".some, 11, 89.11, Striker)
    val player2 = OptionalPlayer(None, 25, 79.33, Goalkeeper)
    val team    = OptionalTeam("the_best".some, List(player1, player2).some)
    OptionalTeam.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "name")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) => fail(s"Should not fail. $value")
      case Right(value) =>
        OptionalTeam.format.from(value) shouldBe OptionalTeam(
          "the_best".some,
          List(player1.copy(name = obfuscation.constant.some), player2).some,
        )
    }
  }

  test(
    s"returns an error when the obfuscation path resolves to a non string when parent path involves nullable fields",
  ) {
    val player1 = OptionalPlayer("James ItsAllGood".some, 11, 89.11, Striker)
    val player2 = OptionalPlayer(None, 25, 79.33, Goalkeeper)
    val team    = OptionalTeam("the_best".some, List(player1, player2).some)
    OptionalTeam.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "number")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) =>
        value shouldBe "Invalid obfuscation path: players.value.number. Path: players.value.number resolves to INT which is not allowed."
      case Right(_) => fail("Should fail")
    }
  }

  test(s"fails when it does not have 'value' for array field") {
    val player1 = Player("James ItsAllGood", 11, 89.11, Striker)
    val player2 = Player("Mark Green", 25, 79.33, Goalkeeper)
    val team    = Team("the_best", List(player1, player2))
    Team.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "name")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) =>
        value shouldBe "Invalid obfuscation path:players.name. The value resolves to an array and it requires: value. For example: playersName.value (when playersName is a sequence of strings), players.value.name."
      case Right(_) => fail("Should fail")
    }
  }

  test(s"fails when the obfuscation resolves to non string") {
    val player1 = Player("James ItsAllGood", 11, 89.11, Striker)
    val player2 = Player("Mark Green", 25, 79.33, Goalkeeper)
    val team    = Team("the_best", List(player1, player2))
    Team.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "number")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) =>
        value shouldBe "Invalid obfuscation path: players.value.number. Path: players.value.number resolves to INT which is not allowed."
      case Right(_) => fail("Should fail")
    }

    Team.format.to(team).asInstanceOf[GenericRecord].obfuscate(
      NonEmptyList.one(ObfuscatedField(NonEmptyList.fromListUnsafe(List("players", "value", "type")))),
      obfuscation,
      Vector.empty,
    ) match {
      case Left(value) =>
        value shouldBe "Invalid obfuscation path: players.value.type. Path: players.value.type resolves to ENUM which is not allowed."
      case Right(_) => fail("Should fail")
    }
  }

}

object PlayerType extends Enumeration {
  val Goalkeeper, Striker, Defender = Value
}

case class Player(name: String, number: Int, efficacy: Double, `type`: PlayerType.Value)

object Player {
  val format: RecordFormat[Player] = RecordFormat[Player]
}

case class Team(name: String, players: List[Player])

object Team {
  val format: RecordFormat[Team] = RecordFormat[Team]
}

case class OptionalPlayer(name: Option[String], number: Int, efficacy: Double, `type`: PlayerType.Value)

object OptionalPlayer {
  val format: RecordFormat[OptionalPlayer] = RecordFormat[OptionalPlayer]
}

case class OptionalTeam(name: Option[String], players: Option[List[OptionalPlayer]])

object OptionalTeam {
  val format: RecordFormat[OptionalTeam] = RecordFormat[OptionalTeam]
}
