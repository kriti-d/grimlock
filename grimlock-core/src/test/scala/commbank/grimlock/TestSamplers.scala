// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.test

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.sample._

import scala.util.Random

import shapeless.{ ::, HList, HNil }
import shapeless.nat.{ _0, _1 }

trait TestSample extends TestGrimlock {
  val con = Content(ContinuousSchema[Double](), 3.14)

  def toCell[P <: HList](pos: Position[P]): Cell[P] = Cell(pos, con)
}

class TestRandomSample extends TestSample {
  type P = Value[Int] :: HNil

  "A RandomSample" should "select 25% correctly" in {
    val obj = RandomSample[P](0.25, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = RandomSample[P](0.5, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = RandomSample[P](0.75, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 7500 +- 50
  }
}

class TestHashSample extends TestSample {
  type P = Value[Int] :: Value[Int] :: HNil

  "A HashSample" should "select 25% correctly" in {
    val obj = HashSample[P, _1](_1, 1, 4)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSample[P, _1](_1, 5, 10)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 5000 +- 100
  }

  it should "select 75% correctly" in {
    val obj = HashSample[P, _1](_1, 75, 100)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 7500 +- 100
  }
}

class TestHashSampleToSize extends TestSample {
  type P = Value[Int] :: Value[Int] :: HNil

  "A HashSampleToSize" should "select 25% correctly" in {
    val obj = HashSampleToSize(
      _1,
      ExtractWithKey[P, Int, Content](2).andThenPresent(_.value.as[Double]),
      2500
    )
    val ext = Map(Position(2) -> Content(DiscreteSchema[Long](), 10000L))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSampleToSize(
      _1,
      ExtractWithKey[P, Int, Content](2).andThenPresent(_.value.as[Double]),
      5000
    )
    val ext = Map(Position(2) -> Content(DiscreteSchema[Long](), 10000L))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = HashSampleToSize(
      _1,
      ExtractWithKey[P, Int, Content](2).andThenPresent(_.value.as[Double]),
      7500
    )
    val ext = Map(Position(2) -> Content(DiscreteSchema[Long](), 10000L))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 7500 +- 100
  }
}

class TestAndThenSampler extends TestSample {
  type P = Value[Int] :: Value[Int] :: HNil

  "A AndThenSampler" should "select correctly" in {
    val obj = HashSample[P, _1](_1, 1, 4).andThen(HashSample(_0, 1, 4))
    val res = (1 to 10000).flatMap(i => if (obj.select(toCell(Position(i, i)))) Option((i, i)) else None)

    res.map(_._1).distinct.length shouldBe 2500 +- 50
    res.map(_._2).distinct.length shouldBe 2500 +- 50
  }
}

class TestAndThenSamplerWithValue extends TestSample {
  type P = Value[Int] :: Value[Int] :: HNil

  "A AndThenSamplerWithValue" should "select correctly" in {
    val obj = HashSampleToSize(
      _1,
      ExtractWithKey[P, Int, Content](2).andThenPresent(_.value.as[Double]),
      2500
    )
      .andThenWithValue(HashSample(_0, 1, 4))
    val ext = Map(Position(2) -> Content(DiscreteSchema[Long](), 10000L))
    val res = (1 to 10000).flatMap(i => if (obj.selectWithValue(toCell(Position(i, i)), ext)) Option((i, i)) else None)

    res.map(_._1).distinct.length shouldBe 625 +- 50
    res.map(_._2).distinct.length shouldBe 625 +- 50
  }
}

