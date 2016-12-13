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

import commbank.grimlock.library.squash._

import shapeless.nat.{ _1, _2, _3 }

trait TestSquashers extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema[Long](), 123)
  val con2 = Content(ContinuousSchema[Long](), 456)
  val cell1 = Cell(Position(1, "b", DateValue(dfmt.parse("2001-01-01"), DateCodec())), con1)
  val cell2 = Cell(Position(2, "a", DateValue(dfmt.parse("2002-01-01"), DateCodec())), con2)
}

class TestPreservingMaxPosition extends TestSquashers {

  "A PreservingMaxPosition" should "return the second cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the first dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the second dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe Option((cell1.position(_3), cell1.content))

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe Option((cell2.position(_3), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when greater" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell2, _3)
    t1 shouldBe Option((cell2.position(_3), cell2.content))

    val t2 = squash.prepare(cell1, _3)
    t2 shouldBe Option((cell1.position(_3), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMaxPosition[_3]()

    val t1 = squash.prepare(cell2, _3)
    t1 shouldBe Option((cell2.position(_3), cell2.content))

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe Option((cell2.position(_3), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }
}

class TestPreservingMinPosition extends TestSquashers {

  "A PreservingMinPosition" should "return the first cell for the first dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option((cell2.position(_1), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the first dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell2, _1)
    t1 shouldBe Option((cell2.position(_1), cell2.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the first dimension when equal" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option((cell1.position(_1), cell1.content))

    val t2 = squash.prepare(cell1, _1)
    t2 shouldBe Option((cell1.position(_1), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the second dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option((cell1.position(_2), cell1.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell1, _2)
    t2 shouldBe Option((cell1.position(_2), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell2, _2)
    t1 shouldBe Option((cell2.position(_2), cell2.content))

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option((cell2.position(_2), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell2.content)
  }

  it should "return the first cell for the third dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe Option((cell1.position(_3), cell1.content))

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe Option((cell2.position(_3), cell2.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when less" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell2, _3)
    t1 shouldBe Option((cell2.position(_3), cell2.content))

    val t2 = squash.prepare(cell1, _3)
    t2 shouldBe Option((cell1.position(_3), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = PreservingMinPosition[_3]()

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe Option((cell1.position(_3), cell1.content))

    val t2 = squash.prepare(cell1, _3)
    t2 shouldBe Option((cell1.position(_3), cell1.content))

    val t = squash.reduce(t1.get, t2.get)

    squash.present(t) shouldBe Option(cell1.content)
  }
}

class TestKeepSlice extends TestSquashers {

  "A KeepSlice" should "return the first cell for the first dimension when equal" in {
    val squash = KeepSlice[_3](1)

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the first dimension when equal" in {
    val squash = KeepSlice[_3](2)

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the first dimension when not equal" in {
    val squash = KeepSlice[_3](3)

    val t1 = squash.prepare(cell1, _1)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _1)
    t2 shouldBe None
  }

  it should "return the second cell for the second dimension when equal" in {
    val squash = KeepSlice[_3]("b")

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the first cell for the second dimension when equal" in {
    val squash = KeepSlice[_3]("a")

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the second dimension when not equal" in {
    val squash = KeepSlice[_3]("c")

    val t1 = squash.prepare(cell1, _2)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _2)
    t2 shouldBe None
  }

  it should "return the first cell for the third dimension when equal" in {
    val squash = KeepSlice[_3](DateValue(dfmt.parse("2001-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe Option(cell1.content)

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe None

    squash.present(t1.get) shouldBe Option(cell1.content)
  }

  it should "return the second cell for the third dimension when equal" in {
    val squash = KeepSlice[_3](DateValue(dfmt.parse("2002-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe Option(cell2.content)

    squash.present(t2.get) shouldBe Option(cell2.content)
  }

  it should "return the second cell for the third dimension when not equal" in {
    val squash = KeepSlice[_3](DateValue(dfmt.parse("2003-01-01"), DateCodec()))

    val t1 = squash.prepare(cell1, _3)
    t1 shouldBe None

    val t2 = squash.prepare(cell2, _3)
    t2 shouldBe None
  }
}

