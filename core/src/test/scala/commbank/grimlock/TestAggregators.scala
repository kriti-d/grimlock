// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import shapeless.Nat
import shapeless.nat.{ _1, _2 }
import shapeless.ops.nat.{ LTEq, ToInt }

trait TestAggregators extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)
}

class TestCount extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))

  "A Count" should "prepare, reduce and present" in {
    val obj = Count[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Count[_2, _1]().andThenRelocate(_.position.append("count").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "count"), getLongContent(2)))
  }
}

class TestMean extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Mean" should "prepare, reduce and present" in {
    val obj = Mean[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Mean[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Mean[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Mean[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Mean[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Mean[_2, _1]().andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Mean[_2, _1](false, true, true).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "mean")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Mean[_2, _1](false, true, false).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Mean[_2, _1](false, false, true).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Mean[_2, _1](false, false, false).andThenRelocate(_.position.append("mean").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "mean"), getDoubleContent(1.5)))
  }

  it should "filter" in {
    Mean[_2, _1](true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Mean[_2, _1](false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Mean[_2, _1](true).prepare(cell3) shouldBe None
    Mean[_2, _1](false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestStandardDeviation extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A StandardDeviation" should "prepare, reduce and present" in {
    val obj = StandardDeviation[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = StandardDeviation[_2, _1](true, false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = StandardDeviation[_2, _1](false, false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = StandardDeviation[_2, _1](true, false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = StandardDeviation[_2, _1](false, false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = StandardDeviation[_2, _1]().andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.7071067811865476)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = StandardDeviation[_2, _1](true, false, true, true).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "sd")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = StandardDeviation[_2, _1](false, false, true, false).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = StandardDeviation[_2, _1](true, false, false, true).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.5)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = StandardDeviation[_2, _1](false, false, false, false).andThenRelocate(_.position.append("sd").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sd"), getDoubleContent(0.7071067811865476)))
  }

  it should "filter" in {
    StandardDeviation[_2, _1](filter=true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    StandardDeviation[_2, _1](filter=false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    StandardDeviation[_2, _1](filter=true).prepare(cell3) shouldBe None
    StandardDeviation[_2, _1](filter=false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestSkewness extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Skewness" should "prepare, reduce and present" in {
    val obj = Skewness[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Skewness[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Skewness[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Skewness[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Skewness[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Skewness[_2, _1]().andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Skewness[_2, _1](false, true, true).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "skewness")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Skewness[_2, _1](false, true, false).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Skewness[_2, _1](false, false, true).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Skewness[_2, _1](false, false, false).andThenRelocate(_.position.append("skewness").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "skewness"), getDoubleContent(0)))
  }

  it should "filter" in {
    Skewness[_2, _1](true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Skewness[_2, _1](false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Skewness[_2, _1](true).prepare(cell3) shouldBe None
    Skewness[_2, _1](false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestKurtosis extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Kurtosis" should "prepare, reduce and present" in {
    val obj = Kurtosis[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Kurtosis[_2, _1](true, false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Kurtosis[_2, _1](false, false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Kurtosis[_2, _1](true, false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Kurtosis[_2, _1](false, false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Kurtosis[_2, _1]().andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Kurtosis[_2, _1](true, false, true, true).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "kurtosis")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Kurtosis[_2, _1](false, false, true, false).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2.mean.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Kurtosis[_2, _1](true, false, false, true).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(-2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Kurtosis[_2, _1](false, false, false, false).andThenRelocate(_.position.append("kurtosis").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(com.twitter.algebird.Moments(1))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(com.twitter.algebird.Moments(2))

    val t3 = obj.prepare(cell3)
    t3.map(_.mean.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe com.twitter.algebird.Moments(2, 1.5, 0.5, 0.0, 0.125)

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "kurtosis"), getDoubleContent(1)))
  }

  it should "filter" in {
    Kurtosis[_2, _1](filter=true).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Kurtosis[_2, _1](filter=false).prepare(cell1) shouldBe Option(com.twitter.algebird.Moments(1))
    Kurtosis[_2, _1](filter=true).prepare(cell3) shouldBe None
    Kurtosis[_2, _1](filter=false).prepare(cell3).map(_.mean.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMin extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Min" should "prepare, reduce and present" in {
    val obj = Min[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 1

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Min[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Min[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Min[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Min[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Min[_2, _1]().andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 1

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Min[_2, _1](false, true, true).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "min")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Min[_2, _1](false, true, false).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Min[_2, _1](false, false, true).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Min[_2, _1](false, false, false).andThenRelocate(_.position.append("min").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 1

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 1

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "min"), getDoubleContent(1)))
  }

  it should "filter" in {
    Min[_2, _1](true).prepare(cell1) shouldBe Option(1)
    Min[_2, _1](false).prepare(cell1) shouldBe Option(1)
    Min[_2, _1](true).prepare(cell3) shouldBe None
    Min[_2, _1](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMax extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Max" should "prepare, reduce and present" in {
    val obj = Max[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Max[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Max[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Max[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Max[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Max[_2, _1]().andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Max[_2, _1](false, true, true).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "max")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Max[_2, _1](false, true, false).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Max[_2, _1](false, false, true).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Max[_2, _1](false, false, false).andThenRelocate(_.position.append("max").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max"), getDoubleContent(2)))
  }

  it should "filter" in {
    Max[_2, _1](true).prepare(cell1) shouldBe Option(1)
    Max[_2, _1](false).prepare(cell1) shouldBe Option(1)
    Max[_2, _1](true).prepare(cell3) shouldBe None
    Max[_2, _1](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestMaxAbs extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(-2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A MaxAbs" should "prepare, reduce and present" in {
    val obj = MaxAbs[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = MaxAbs[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = MaxAbs[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = MaxAbs[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = MaxAbs[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = MaxAbs[_2, _1]().andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = MaxAbs[_2, _1](false, true, true).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "max.abs")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = MaxAbs[_2, _1](false, true, false).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = MaxAbs[_2, _1](false, false, true).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = MaxAbs[_2, _1](false, false, false).andThenRelocate(_.position.append("max.abs").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(-2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 2

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 2

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "max.abs"), getDoubleContent(2)))
  }

  it should "filter" in {
    MaxAbs[_2, _1](true).prepare(cell1) shouldBe Option(1)
    MaxAbs[_2, _1](false).prepare(cell1) shouldBe Option(1)
    MaxAbs[_2, _1](true).prepare(cell3) shouldBe None
    MaxAbs[_2, _1](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestSum extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(2))
  val cell3 = Cell(Position("foo", "bar"), getStringContent("bar"))

  "A Sum" should "prepare, reduce and present" in {
    val obj = Sum[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 3

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = Sum[_2, _1](false, true, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = Sum[_2, _1](false, true, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = Sum[_2, _1](false, false, true)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = Sum[_2, _1](false, false, false)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Sum[_2, _1]().andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 3

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = Sum[_2, _1](false, true, true).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result.get
    c.position shouldBe Position("foo", "sum")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = Sum[_2, _1](false, true, false).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.present(Position("foo"), r2).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = Sum[_2, _1](false, false, true).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = Sum[_2, _1](false, false, false).andThenRelocate(_.position.append("sum").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(2)

    val t3 = obj.prepare(cell3)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3

    val c = obj.present(Position("foo"), r2).result
    c shouldBe Option(Cell(Position("foo", "sum"), getDoubleContent(3)))
  }

  it should "filter" in {
    Sum[_2, _1](true).prepare(cell1) shouldBe Option(1)
    Sum[_2, _1](false).prepare(cell1) shouldBe Option(1)
    Sum[_2, _1](true).prepare(cell3) shouldBe None
    Sum[_2, _1](false).prepare(cell3).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestPredicateCount extends TestAggregators {

  val cell1 = Cell(Position("foo", "one"), getDoubleContent(-1))
  val cell2 = Cell(Position("foo", "two"), getDoubleContent(0))
  val cell3 = Cell(Position("foo", "three"), getDoubleContent(1))
  val cell4 = Cell(Position("foo", "bar"), getStringContent("bar"))

  def predicate(con: Content) = con.value.asDouble.map(_ <= 0).getOrElse(false)

  "A PredicateCount" should "prepare, reduce and present" in {
    val obj = PredicateCount[_2, _1](predicate)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(1)

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(1)

    val t3 = obj.prepare(cell3)
    t3 shouldBe None

    val t4 = obj.prepare(cell4)
    t4 shouldBe None

    val r = obj.reduce(t1.get, t2.get)
    r shouldBe 2

    val c = obj.present(Position("foo"), r).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(2)))
  }
}

class TestWeightedSum extends TestAggregators {

  val cell1 = Cell(Position("foo", 1), getDoubleContent(-1))
  val cell2 = Cell(Position("bar", 2), getDoubleContent(1))
  val cell3 = Cell(Position("xyz", 3), getStringContent("abc"))
  val ext = Map(
    Position("foo") -> 3.14,
    Position("bar") -> 6.28,
    Position(2) -> 3.14,
    Position("foo.model1") -> 3.14,
    Position("bar.model1") -> 6.28,
    Position("2.model2") -> -3.14
  )

  type W = Map[Position[_1], Double]

  def extractor1 = ExtractWithDimension[_2, Double](_1)
  def extractor2 = ExtractWithDimension[_2, Double](_2)

  case class ExtractWithName[
    D <: Nat : ToInt
  ](
    dim: D,
    name: String
  )(implicit
    ev: LTEq[D, _2]
  ) extends Extract[_2, W, Double] {
    def extract(cell: Cell[_2], ext: W): Option[Double] = ext
      .get(Position(name.format(cell.position(dim).toShortString)))
  }

  "A WeightedSum" should "prepare, reduce and present on the first dimension" in {
    val obj = WeightedSum[_2, _1, W](extractor1)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present on the second dimension" in {
    val obj = WeightedSum[_2, _1, W](extractor2)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with strict and nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, true, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict and non-nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, true, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict and nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, false, true)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present with non-strict and non-nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, false, false)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the first dimension" in {
    val obj = WeightedSum[_2, _1, W](extractor1)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded on the second dimension" in {
    val obj = WeightedSum[_2, _1, W](extractor2)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with strict and nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, true, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo", "result")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict and non-nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, true, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict and nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, false, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present expanded with non-strict and non-nan" in {
    val obj = WeightedSum[_2, _1, W](extractor1, false, false, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the first dimension with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_1, "%1$s.model1"))
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple on the second dimension with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_2, "%1$s.model2"))
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(0)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(-3.14)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe -3.14

    val c = obj.presentWithValue(Position("foo"), r1, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(-3.14)))
  }

  it should "prepare, reduce and present multiple with strict and nan with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_1, "%1$s.model1"), false, true, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result.get
    c.position shouldBe Position("foo", "result")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present multiple with strict and non-nan with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_1, "%1$s.model1"), false, true, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe None
  }

  it should "prepare, reduce and present multiple with non-strict and nan with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_1, "%1$s.model1"), false, false, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "prepare, reduce and present multiple with non-strict and non-nan with format" in {
    val obj = WeightedSum[_2, _1, W](ExtractWithName(_1, "%1$s.model1"), false, false, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("result").toOption)

    val t1 = obj.prepareWithValue(cell1, ext)
    t1 shouldBe Option(-3.14)

    val t2 = obj.prepareWithValue(cell2, ext)
    t2 shouldBe Option(6.28)

    val t3 = obj.prepareWithValue(cell3, ext)
    t3.map(_.compare(Double.NaN)) shouldBe Option(0)

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe 3.14

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe 3.14

    val c = obj.presentWithValue(Position("foo"), r2, ext).result
    c shouldBe Option(Cell(Position("foo", "result"), getDoubleContent(3.14)))
  }

  it should "filter" in {
    WeightedSum[_2, _1, W](extractor1, true).prepareWithValue(cell1, ext) shouldBe Option(-3.14)
    WeightedSum[_2, _1, W](extractor1, false).prepareWithValue(cell1, ext) shouldBe Option(-3.14)
    WeightedSum[_2, _1, W](extractor1, true).prepareWithValue(cell3, ext) shouldBe None
    WeightedSum[_2, _1, W](extractor1, false).prepareWithValue(cell3, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
  }
}

class TestDistinctCount extends TestAggregators {

  val cell1 = Cell(Position("foo", 1), getDoubleContent(1))
  val cell2 = Cell(Position("foo", 2), getDoubleContent(1))
  val cell3 = Cell(Position("foo", 3), getDoubleContent(1))
  val cell4 = Cell(Position("abc", 4), getStringContent("abc"))
  val cell5 = Cell(Position("xyz", 4), getStringContent("abc"))
  val cell6 = Cell(Position("bar", 5), getLongContent(123))

  "A DistinctCount" should "prepare, reduce and present" in {
    val obj = DistinctCount[_2, _1]()

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Set(DoubleValue(1)))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Set(DoubleValue(1)))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Set(DoubleValue(1)))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Set(StringValue("abc")))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Option(Set(StringValue("abc")))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Option(Set(LongValue(123)))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6.get)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position("foo"), r5).result
    c shouldBe Option(Cell(Position("foo"), getLongContent(3)))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = DistinctCount[_2, _1]().andThenRelocate(_.position.append("count").toOption)

    val t1 = obj.prepare(cell1)
    t1 shouldBe Option(Set(DoubleValue(1)))

    val t2 = obj.prepare(cell2)
    t2 shouldBe Option(Set(DoubleValue(1)))

    val t3 = obj.prepare(cell3)
    t3 shouldBe Option(Set(DoubleValue(1)))

    val t4 = obj.prepare(cell4)
    t4 shouldBe Option(Set(StringValue("abc")))

    val t5 = obj.prepare(cell5)
    t5 shouldBe Option(Set(StringValue("abc")))

    val t6 = obj.prepare(cell6)
    t6 shouldBe Option(Set(LongValue(123)))

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe Set(DoubleValue(1))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe Set(DoubleValue(1))

    val r3 = obj.reduce(r2, t4.get)
    r3 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r4 = obj.reduce(r3, t5.get)
    r4 shouldBe Set(DoubleValue(1), StringValue("abc"))

    val r5 = obj.reduce(r4, t6.get)
    r5 shouldBe Set(DoubleValue(1), StringValue("abc"), LongValue(123))

    val c = obj.present(Position("foo"), r5).result
    c shouldBe Option(Cell(Position("foo", "count"), getLongContent(3)))
  }
}

class TestEntropy extends TestAggregators {

  val cell1 = Cell(Position("foo", "abc"), getLongContent(1))
  val cell2 = Cell(Position("foo", "xyz"), getLongContent(2))
  val cell3 = Cell(Position("foo", "123"), getStringContent("456"))

  def log2(x: Double) = math.log(x) / math.log(2)
  def log4(x: Double) = math.log(x) / math.log(4)

  def extractor = ExtractWithDimension[_2, Double](_1)

  val count = Map(Position("foo") -> 3.0)

  type W = Map[Position[_1], Double]

  "An Entropy" should "prepare, reduce and present" in {
    val obj = Entropy[_2, _1, W](extractor, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present strict, nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present single with strict, non-nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, true)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, false)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, log = log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict, nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present with strict, non-nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with strict, non-nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present with non-strict, nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, true, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, false, log4 _)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded" in {
    val obj = Entropy[_2, _1, W](extractor, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present multiple with strict, nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, true)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, false)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log2(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log2(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log2(2.0/3) + 1.0/3 * log2(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, log = log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, true, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, true, false, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result.get
    c.position shouldBe Position("foo", "entropy")
    c.content.value.asDouble.map(_.compare(Double.NaN)) shouldBe Option(0)
  }

  it should "prepare, reduce and present expanded with strict, non-nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, true, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with strict, non-nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, true, false, false, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2._1 shouldBe 3
    r2._2.compare(Double.NaN) shouldBe 0

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe None
  }

  it should "prepare, reduce and present expanded with non-strict, nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, true, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, true, false, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, true, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))
  }

  it should "prepare, reduce and present expanded with non-strict, non-nan and non-negate with log" in {
    val obj = Entropy[_2, _1, W](extractor, false, false, false, false, log4 _)
      .andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("entropy").toOption)

    val t1 = obj.prepareWithValue(cell1, count)
    t1 shouldBe Option(((1, 1.0/3 * log4(1.0/3))))

    val t2 = obj.prepareWithValue(cell2, count)
    t2 shouldBe Option(((1, 2.0/3 * log4(2.0/3))))

    val t3 = obj.prepareWithValue(cell3, count)
    t3.get._1 shouldBe 1
    t3.get._2.compare(Double.NaN) shouldBe 0

    val r1 = obj.reduce(t1.get, t2.get)
    r1 shouldBe ((2, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val r2 = obj.reduce(r1, t3.get)
    r2 shouldBe ((3, (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3))))

    val c = obj.presentWithValue(Position("foo"), r2, count).result
    c shouldBe Option(Cell(Position("foo", "entropy"), getDoubleContent(- (2.0/3 * log4(2.0/3) + 1.0/3 * log4(1.0/3)))))
  }

  it should "filter" in {
    Entropy[_2, _1, W](extractor, true).prepareWithValue(cell1, count) shouldBe Option(((1, 1.0/3 * log2(1.0/3))))
    Entropy[_2, _1, W](extractor, false).prepareWithValue(cell1, count) shouldBe Option(((1, 1.0/3 * log2(1.0/3))))
    Entropy[_2, _1, W](extractor, true).prepareWithValue(cell3, count) shouldBe None
    Entropy[_2, _1, W](extractor, false)
      .prepareWithValue(cell3, count)
      .map { case (l, d) => (l, d.compare(Double.NaN)) } shouldBe Option((1, 0))
  }
}

class TestWithPrepareAggregator extends TestAggregators {

  val str = Cell(Position("x"), getStringContent("foo"))
  val dbl = Cell(Position("y"), getDoubleContent(3.14))
  val lng = Cell(Position("z"), getLongContent(42))

  val ext = Map(
    Position("x") -> getDoubleContent(1),
    Position("y") -> getDoubleContent(2),
    Position("z") -> getDoubleContent(3)
  )

  def prepare(cell: Cell[_1]): Content = cell.content.value match {
    case l: LongValue => cell.content
    case d: DoubleValue => getStringContent("not.supported")
    case s: StringValue => getLongContent(s.value.length)
  }

  def prepareWithValue(cell: Cell[_1], ext: Map[Position[_1], Content]): Content =
    (cell.content.value, ext(cell.position).value) match {
      case (l: LongValue, d: DoubleValue) => getLongContent(l.value * d.value.toLong)
      case (d: DoubleValue, _) => getStringContent("not.supported")
      case (s: StringValue, _) => getLongContent(s.value.length)
    }

  "An Aggregator" should "withPrepare prepare correctly" in {
    val obj = Max[_1, _1](false).withPrepare(prepare)

    obj.prepare(str) shouldBe Option(3.0)
    obj.prepare(dbl).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepare(lng) shouldBe Option(42.0)
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = WeightedSum[_1, _1, Map[Position[_1], Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble),
      false
    ).withPrepare(prepare)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepareWithValue(lng, ext) shouldBe Option(3 * 42.0)
  }

  it should "withPrepareWithVaue correctly" in {
    val obj = WeightedSum[_1, _1, Map[Position[_1], Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble),
      false
    ).withPrepareWithValue(prepareWithValue)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext).map(_.compare(Double.NaN)) shouldBe Option(0)
    obj.prepareWithValue(lng, ext) shouldBe Option(3 * 3 * 42.0)
  }
}

class TestAndThenMutateAggregator extends TestAggregators {

  val x = Position("x")
  val y = Position("y")
  val z = Position("z")

  val ext = Map(x -> getDoubleContent(3), y -> getDoubleContent(2), z -> getDoubleContent(1))

  def mutate(cell: Cell[_1]): Content = cell.position match {
    case Position(StringValue("x", _)) => cell.content
    case Position(StringValue("y", _)) => getStringContent("not.supported")
    case Position(StringValue("z", _)) => getLongContent(123)
  }

  def mutateWithValue(cell: Cell[_1], ext: Map[Position[_1], Content]): Content =
    (cell.position, ext(cell.position).value) match {
      case (Position(StringValue("x", _)), DoubleValue(d, _)) => getStringContent("x" * d.toInt)
      case (Position(StringValue("y", _)), _) => getStringContent("not.supported")
      case (Position(StringValue("z", _)), DoubleValue(d, _)) => getLongContent(d.toLong)
    }

  "An Aggregator" should "andThenMutate correctly" in {
    val obj = Max[_1, _1]().andThenMutate(mutate)

    obj.present(x, -1).result shouldBe Option(Cell(x, getDoubleContent(-1)))
    obj.present(y, 3.14).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.present(z, 42).result shouldBe Option(Cell(z, getLongContent(123)))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = WeightedSum[_1, _1, Map[Position[_1], Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)
    ).andThenMutate(mutate)

    obj.presentWithValue(x, -1, ext).result shouldBe Option(Cell(x, getDoubleContent(-1)))
    obj.presentWithValue(y, 3.14, ext).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.presentWithValue(z, 42, ext).result shouldBe Option(Cell(z, getLongContent(123)))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = WeightedSum[_1, _1, Map[Position[_1], Content]](
      ExtractWithPosition().andThenPresent(_.value.asDouble)
    ).andThenMutateWithValue(mutateWithValue)

    obj.presentWithValue(x, -1, ext).result shouldBe Option(Cell(x, getStringContent("xxx")))
    obj.presentWithValue(y, 3.14, ext).result shouldBe Option(Cell(y, getStringContent("not.supported")))
    obj.presentWithValue(z, 42, ext).result shouldBe Option(Cell(z, getLongContent(1)))
  }
}

