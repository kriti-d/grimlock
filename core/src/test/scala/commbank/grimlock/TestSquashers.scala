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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.squash._

import java.util.Date

import shapeless.{ ::, HNil }
import shapeless.nat.{ _0, _1, _2 }

trait TestSquashers extends TestGrimlock {
  type P = Value[Int] :: Value[String] :: Value[Date] :: HNil

  implicit def toDate(date: Date): DateValue = DateValue(date)

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema[Long](), 123L)
  val con2 = Content(ContinuousSchema[Long](), 456L)
  val cell1 = Cell(Position(1, "b", dfmt.parse("2001-01-01")), con1)
  val cell2 = Cell(Position(2, "a", dfmt.parse("2002-01-01")), con2)
}

class TestPreservingMaximumPosition extends TestSquashers {
  "A PreservingMaximumPosition" should "return the second cell for the first dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe Option((cell1.position(_0), cell1.content))

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe Option((cell2.position(_0), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the first dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell2, _0)
    t1 shouldBe Option((cell2.position(_0), cell2.content))

    val t2 = squash.prepare(cell1, _0)
    t2 shouldBe Option((cell1.position(_0), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell2, _0)
    t1 shouldBe Option((cell2.position(_0), cell2.content))

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe Option((cell2.position(_0), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the second dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when greater" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMaximumPosition[P]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }
}

class TestPreservingMinimumPosition extends TestSquashers {
  "A PreservingMinimumPosition" should "return the first cell for the first dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe Option((cell1.position(_0), cell1.content))

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe Option((cell2.position(_0), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the first dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell2, _0)
    t1 shouldBe Option((cell2.position(_0), cell2.content))

    val t2 = squash.prepare(cell1, _0)
    t2 shouldBe Option((cell1.position(_0), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe Option((cell1.position(_0), cell1.content))

    val t2 = squash.prepare(cell1, _0)
    t2 shouldBe Option((cell1.position(_0), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the second dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when less" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMinimumPosition[P]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }
}

class TestKeepSlice extends TestSquashers {
  "A KeepSlice" should "return the first cell for the first dimension when equal" in {
    val squash = KeepSlice[P](1)

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the first dimension when equal" in {
    val squash = KeepSlice[P](2)

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the first dimension when not equal" in {
    val squash = KeepSlice[P](3)

    val t1 = squash.prepare(cell1, _0)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _0)
    t2 shouldBe None
  }

  it should "return the second cell for the second dimension when equal" in {
    val squash = KeepSlice[P]("b")

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = KeepSlice[P]("a")

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the second dimension when not equal" in {
    val squash = KeepSlice[P]("c")

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe None
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = KeepSlice[P](DateValue(dfmt.parse("2001-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when equal" in {
    val squash = KeepSlice[P](DateValue(dfmt.parse("2002-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the third dimension when not equal" in {
    val squash = KeepSlice[P](DateValue(dfmt.parse("2003-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe None
  }
}

