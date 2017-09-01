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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _1, _2 }

class TestExtractWithKey extends TestGrimlock {

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("ghi") -> 3.14)

  "A ExtractWithKey" should "extract with key" in {
    ExtractWithKey[_2, Double]("ghi").extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with missing key" in {
    ExtractWithKey[_2, Double]("jkl").extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithKey[_2, Double]("ghi").andThenPresent(d => Option(d * 2)).extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithDimension extends TestGrimlock {

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> 3.14)

  "A ExtractWithDimension" should "extract with _1" in {
    ExtractWithDimension[_2, Double](_1).extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with _2" in {
    ExtractWithDimension[_2, Double](_2).extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithDimension[_2, Double](_1).andThenPresent(d => Option(d * 2)).extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithDimensionAndKey extends TestGrimlock {

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position(123) -> 3.14))

  "A ExtractWithDimensionAndKey" should "extract with _1" in {
    ExtractWithDimensionAndKey[_2, Double](_1, 123).extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with missing key" in {
    ExtractWithDimensionAndKey[_2, Double](_1, 456).extract(cell, ext) shouldBe None
  }

  it should "extract with _2" in {
    ExtractWithDimensionAndKey[_2, Double](_2, 123).extract(cell, ext) shouldBe None
  }

  it should "extract with _2 and missing key" in {
    ExtractWithDimensionAndKey[_2, Double](_2, 456).extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithDimensionAndKey[_2, Double](_1, 123)
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithPosition extends TestGrimlock {

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc", "def") -> 3.14)

  "A ExtractWithPosition" should "extract with key" in {
    ExtractWithPosition[_2, Double]().extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with missing position" in {
    ExtractWithPosition[_2, Double]().extract(cell2, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithPosition[_2, Double]().andThenPresent(d => Option(d * 2)).extract(cell1, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithPositionAndKey extends TestGrimlock {

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc", "def") -> Map(Position("xyz") -> 3.14))

  "A ExtractWithPositionAndKey" should "extract with key" in {
    ExtractWithPositionAndKey[_2, Double]("xyz").extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with missing position" in {
    ExtractWithPositionAndKey[_2, Double]("xyz").extract(cell2, ext) shouldBe None
  }

  it should "extract with missing key" in {
    ExtractWithPositionAndKey[_2, Double]("abc").extract(cell1, ext) shouldBe None
  }

  it should "extract with missing position and key" in {
    ExtractWithPositionAndKey[_2, Double]("abc").extract(cell2, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithPositionAndKey[_2, Double]("xyz")
      .andThenPresent(d => Option(d * 2))
      .extract(cell1, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSelected extends TestGrimlock {

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> 3.14)

  "A ExtractWithSelected" should "extract with Over" in {
    ExtractWithSelected[_2, Double](Over(_1)).extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSelected[_2, Double](Along(_1)).extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSelected[_2, Double](Over(_1))
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSelectedAndKey extends TestGrimlock {

  val cell = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position("xyz") -> 3.14))

  "A ExtractWithSelectedAndKey" should "extract with Over" in {
    ExtractWithSelectedAndKey[_2, Double](Over(_1), "xyz").extract(cell, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSelectedAndKey[_2, Double](Along(_1), "xyz").extract(cell, ext) shouldBe None
  }

  it should "extract with missing key" in {
    ExtractWithSelectedAndKey[_2, Double](Over(_1), "abc").extract(cell, ext) shouldBe None
  }

  it should "extract with Along and missing key" in {
    ExtractWithSelectedAndKey[_2, Double](Along(_1), "abc").extract(cell, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSelectedAndKey[_2, Double](Over(_1), "xyz")
      .andThenPresent(d => Option(d * 2))
      .extract(cell, ext) shouldBe Option(6.28)
  }
}

class TestExtractWithSlice extends TestGrimlock {

  val cell1 = Cell(Position("abc", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell2 = Cell(Position("cba", "def"), Content(ContinuousSchema[Double](), 1.0))
  val cell3 = Cell(Position("abc", "fed"), Content(ContinuousSchema[Double](), 1.0))
  val ext = Map(Position("abc") -> Map(Position("def") -> 3.14))

  "A ExtractWithSlice" should "extract with Over" in {
    ExtractWithSlice[_2, Double](Over(_1)).extract(cell1, ext) shouldBe Option(3.14)
  }

  it should "extract with Along" in {
    ExtractWithSlice[_2, Double](Along(_1)).extract(cell1, ext) shouldBe None
  }

  it should "extract with missing selected" in {
    ExtractWithSlice[_2, Double](Over(_1)).extract(cell2, ext) shouldBe None
  }

  it should "extract with missing remaider" in {
    ExtractWithSlice[_2, Double](Over(_1)).extract(cell3, ext) shouldBe None
  }

  it should "extract and present" in {
    ExtractWithSlice[_2, Double](Over(_1))
      .andThenPresent(d => Option(d * 2))
      .extract(cell1, ext) shouldBe Option(6.28)
  }
}

