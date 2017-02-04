// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.sample._

import scala.util._

import shapeless.Nat
import shapeless.nat.{ _1, _2 }

trait TestSample extends TestGrimlock {

  val con = Content(ContinuousSchema[Double](), 3.14)

  def toCell[P <: Nat](pos: Position[P]): Cell[P] = Cell(pos, con)
}

class TestRandomSample extends TestSample {

  "A RandomSample" should "select 25% correctly" in {
    val obj = RandomSample[_1](0.25, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = RandomSample[_1](0.5, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = RandomSample[_1](0.75, new Random(123))

    (1 to 10000).map(i => if (obj.select(toCell(Position(i)))) 1 else 0).sum shouldBe 7500 +- 50
  }
}

class TestHashSample extends TestSample {

  "A HashSample" should "select 25% correctly" in {
    val obj = HashSample[_2, _2](_2, 1, 4)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSample[_2, _2](_2, 5, 10)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 5000 +- 100
  }

  it should "select 75% correctly" in {
    val obj = HashSample[_2, _2](_2, 75, 100)

    (1 to 10000).map(i => if (obj.select(toCell(Position(2 * i, i)))) 1 else 0).sum shouldBe 7500 +- 50
  }
}

class TestHashSampleToSize extends TestSample {

  "A HashSampleToSize" should "select 25% correctly" in {
    val obj = HashSampleToSize(
      _2,
      ExtractWithKey[_2, Content](Nat.toInt[_2]).andThenPresent(_.value.asDouble),
      2500
    )
    val ext = Map(Position(Nat.toInt[_2]) -> Content(DiscreteSchema[Long](), 10000))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 2500 +- 50
  }

  it should "select 50% correctly" in {
    val obj = HashSampleToSize(
      _2,
      ExtractWithKey[_2, Content](Nat.toInt[_2]).andThenPresent(_.value.asDouble),
      5000
    )
    val ext = Map(Position(Nat.toInt[_2]) -> Content(DiscreteSchema[Long](), 10000))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 5000 +- 50
  }

  it should "select 75% correctly" in {
    val obj = HashSampleToSize(
      _2,
      ExtractWithKey[_2, Content](Nat.toInt[_2]).andThenPresent(_.value.asDouble),
      7500
    )
    val ext = Map(Position(Nat.toInt[_2]) -> Content(DiscreteSchema[Long](), 10000))

    (1 to 10000).map(i => if (obj.selectWithValue(toCell(Position(2 * i, i)), ext)) 1 else 0).sum shouldBe 7500 +- 50
  }
}

class TestAndThenSampler extends TestSample {

  "A AndThenSampler" should "select correctly" in {
    val obj = HashSample[_2, _2](_2, 1, 4).andThen(HashSample(_1, 1, 4))
    val res = (1 to 10000).flatMap(i => if (obj.select(toCell(Position(i, i)))) Option((i, i)) else None)

    res.map(_._1).distinct.length shouldBe 2500 +- 50
    res.map(_._2).distinct.length shouldBe 2500 +- 50
  }
}

class TestAndThenSamplerWithValue extends TestSample {

  "A AndThenSamplerWithValue" should "select correctly" in {
    val obj = HashSampleToSize(
      _2,
      ExtractWithKey[_2, Content](Nat.toInt[_2]).andThenPresent(_.value.asDouble),
      2500
    )
      .andThenWithValue(HashSample(_1, 1, 4))
    val ext = Map(Position(Nat.toInt[_2]) -> Content(DiscreteSchema[Long](), 10000))
    val res = (1 to 10000).flatMap(i => if (obj.selectWithValue(toCell(Position(i, i)), ext)) Option((i, i)) else None)

    res.map(_._1).distinct.length shouldBe 625 +- 50
    res.map(_._2).distinct.length shouldBe 625 +- 50
  }
}

