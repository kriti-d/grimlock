// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

import shapeless.{ ::, HNil }
import shapeless.nat.{ _0, _1 }

class TestExtractWithDimension extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> 3.14)

  "A ExtractWithDimension" should "extract with _0" in {
    ExtractWithDimension[P, _0, Double].extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with _1" in {
    ExtractWithDimension[P, _1, Double].extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithDimension[P, _0, Double].andThenPresent(d => Option(d * 2)).extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithDimensionAndKey extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position(123) -> 3.14))

  "A ExtractWithDimensionAndKey" should "extract with _0" in {
    ExtractWithDimensionAndKey[P, _0, Int, Double](123).extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with missing key" in {
    ExtractWithDimensionAndKey[P, _0, Int, Double](456).extract(cell, ext) shouldBe None
  }

  it should "extract with _1" in {
    ExtractWithDimensionAndKey[P, _1, Int, Double](123).extract(cell, ext) shouldBe None
  }

  it should "extract with _1 and missing key" in {
    ExtractWithDimensionAndKey[P, _1, Int, Double](456).extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithDimensionAndKey[P, _0, Int, Double](123)
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithKey extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("ghi") -> 3.14)

  "A ExtractWithKey" should "extract with key" in {
    ExtractWithKey[P, String, Double]("ghi").extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with missing key" in {
    ExtractWithKey[P, String, Double]("jkl").extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithKey[P, String, Double]("ghi").andThenPresent(d => Option(d * 2)).extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithPosition extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc", "def") -> 3.14)

  "A ExtractWithPosition" should "extract with key" in {
    ExtractWithPosition[P, Double]().extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with missing position" in {
    ExtractWithPosition[P, Double]().extract(cell2, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithPosition[P, Double]().andThenPresent(d => Option(d * 2)).extract(cell1, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithPositionAndKey extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc", "def") -> Map(Position("xyz") -> 3.14))

  "A ExtractWithPositionAndKey" should "extract with key" in {
    ExtractWithPositionAndKey[P, String, Double]("xyz").extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with missing position" in {
    ExtractWithPositionAndKey[P, String, Double]("xyz").extract(cell2, ext) shouldBe None
  }

  it should "extract with missing key" in {
    ExtractWithPositionAndKey[P, String, Double]("abc").extract(cell1, ext) shouldBe None
  }

  it should "extract with missing position and key" in {
    ExtractWithPositionAndKey[P, String, Double]("abc").extract(cell2, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithPositionAndKey[P, String, Double]("xyz")
      .andThenPresent(d => Option(d * 2))
      .extract(cell1, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSelected extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil
  type S = Value[String] :: HNil
  type R = Value[String] :: HNil

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> 3.14)

  "A ExtractWithSelected" should "extract with Over" in {
    ExtractWithSelected[P, S, R, Double](Over(_0)).extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSelected[P, S, R, Double](Along(_0)).extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSelected[P, S, R, Double](Over(_0))
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSelectedAndKey extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil
  type S = Value[String] :: HNil
  type R = Value[String] :: HNil

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position("xyz") -> 3.14))

  "A ExtractWithSelectedAndKey" should "extract with Over" in {
    ExtractWithSelectedAndKey[P, S, R, String, Double](Over(_0), "xyz").extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSelectedAndKey[P, S, R, String, Double](Along(_0), "xyz").extract(cell, ext) shouldBe None
  }

  it should "extract with missing key" in {
    ExtractWithSelectedAndKey[P, S, R, String, Double](Over(_0), "abc").extract(cell, ext) shouldBe None
  }

  it should "extract with Along and missing key" in {
    ExtractWithSelectedAndKey[P, S, R, String, Double](Along(_0), "abc").extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSelectedAndKey[P, S, R, String, Double](Over(_0), "xyz")
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSlice extends TestGrimlock {
  type P = Value[String] :: Value[String] :: HNil
  type S = Value[String] :: HNil
  type R = Value[String] :: HNil

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell3 = Cell(Position("abc", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position("def") -> 3.14))

  "A ExtractWithSlice" should "extract with Over" in {
    ExtractWithSlice[P, S, R, Double](Over(_0)).extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSlice[P, S, R, Double](Along(_0)).extract(cell1, ext) shouldBe None
  }

  it should "extract with missing selected" in {
    ExtractWithSlice[P, S, R, Double](Over(_0)).extract(cell2, ext) shouldBe None
  }

  it should "extract with missing remaider" in {
    ExtractWithSlice[P, S, R, Double](Over(_0)).extract(cell3, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSlice[P, S, R, Double](Over(_0))
      .andThenPresent(d => Option(d * 2))
      .extract(cell1, ext) shouldBe Option(6.28)
  }
}

