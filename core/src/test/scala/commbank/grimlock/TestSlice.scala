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

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2, _3, _4, _5 }

trait TestSlice extends TestGrimlock {
  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema[Long](), 1)
  val con2 = Content(ContinuousSchema[Long](), 2)
}

trait TestSlicePosition1D extends TestSlice {
  val pos1 = Position(1)
  val pos2 = Position(-1)
}

class TestOverPosition extends TestSlicePosition1D {

  "A Over[Position1D]" should "return a Position1D for the selected dimension" in {
    Over[_0, _1](_1).selected(pos1) shouldBe Position(pos1(_1))
  }

  it should "return a Position0D for the remainder" in {
    Over[_0, _1](_1).remainder(pos1) shouldBe pos1.remove(_1)
  }
}

class TestAlongPosition1D extends TestSlicePosition1D {

  "A Along[Position1D]" should "return a Position0D for the selected dimension" in {
    Along[_0, _1](_1).selected(pos1) shouldBe pos1.remove(_1)
  }

  it should "return a Position1D for the remainder" in {
    Along[_0, _1](_1).remainder(pos1) shouldBe Position(pos1(_1))
  }
}

trait TestSlicePosition2D extends TestSlice {
  val pos1 = Position(2, "a")
  val pos2 = Position(-2, "z")
}

class TestOverPosition2D extends TestSlicePosition2D {

  "A Over[Position2D]" should "return a Position1D for the selected dimension" in {
    Over[_1, _2](_1).selected(pos1) shouldBe Position(pos1(_1))
    Over[_1, _2](_2).selected(pos1) shouldBe Position(pos1(_2))
  }

  it should "return a Position1D for the remainder" in {
    Over[_1, _2](_1).remainder(pos1) shouldBe pos1.remove(_1)
    Over[_1, _2](_2).remainder(pos1) shouldBe pos1.remove(_2)
  }
}

class TestAlongPosition2D extends TestSlicePosition2D {

  "A Along[Position2D]" should "return a Position1D for the selected dimension" in {
    Along[_1, _2](_1).selected(pos1) shouldBe pos1.remove(_1)
    Along[_1, _2](_2).selected(pos1) shouldBe pos1.remove(_2)
  }

  it should "return a Position1D for the remainder" in {
    Along[_1, _2](_1).remainder(pos1) shouldBe Position(pos1(_1))
    Along[_1, _2](_2).remainder(pos1) shouldBe Position(pos1(_2))
  }
}

trait TestSlicePosition3D extends TestSlice {
  val pos1 = Position(3, "b", DateValue(dfmt.parse("2001-01-01"), DateCodec()))
  val pos2 = Position(-3, "y", DateValue(dfmt.parse("1999-01-01"), DateCodec()))
}

class TestOverPosition3D extends TestSlicePosition3D {

  "A Over[Position3D]" should "return a Position1D for the selected dimension" in {
    Over[_2, _3](_1).selected(pos1) shouldBe Position(pos1(_1))
    Over[_2, _3](_2).selected(pos1) shouldBe Position(pos1(_2))
    Over[_2, _3](_3).selected(pos1) shouldBe Position(pos1(_3))
  }

  it should "return a Position2D for the remainder" in {
    Over[_2, _3](_1).remainder(pos1) shouldBe pos1.remove(_1)
    Over[_2, _3](_2).remainder(pos1) shouldBe pos1.remove(_2)
    Over[_2, _3](_3).remainder(pos1) shouldBe pos1.remove(_3)
  }
}

class TestAlongPosition3D extends TestSlicePosition3D {

  "A Along[Position3D]" should "return a Position2D for the selected dimension" in {
    Along[_2, _3](_1).selected(pos1) shouldBe pos1.remove(_1)
    Along[_2, _3](_2).selected(pos1) shouldBe pos1.remove(_2)
    Along[_2, _3](_3).selected(pos1) shouldBe pos1.remove(_3)
  }

  it should "return a Position1D for the remainder" in {
    Along[_2, _3](_1).remainder(pos1) shouldBe Position(pos1(_1))
    Along[_2, _3](_2).remainder(pos1) shouldBe Position(pos1(_2))
    Along[_2, _3](_3).remainder(pos1) shouldBe Position(pos1(_3))
  }
}

trait TestSlicePosition4D extends TestSlice {
  val pos1 = Position(4, "c", DateValue(dfmt.parse("2002-01-01"), DateCodec()), "foo")
  val pos2 = Position(-4, "x", DateValue(dfmt.parse("1998-01-01"), DateCodec()), "oof")
}

class TestOverPosition4D extends TestSlicePosition4D {

  "A Over[Position4D]" should "return a Position1D for the selected dimension" in {
    Over[_3, _4](_1).selected(pos1) shouldBe Position(pos1(_1))
    Over[_3, _4](_2).selected(pos1) shouldBe Position(pos1(_2))
    Over[_3, _4](_3).selected(pos1) shouldBe Position(pos1(_3))
    Over[_3, _4](_4).selected(pos1) shouldBe Position(pos1(_4))
  }

  it should "return a Position3D for the remainder" in {
    Over[_3, _4](_1).remainder(pos1) shouldBe pos1.remove(_1)
    Over[_3, _4](_2).remainder(pos1) shouldBe pos1.remove(_2)
    Over[_3, _4](_3).remainder(pos1) shouldBe pos1.remove(_3)
    Over[_3, _4](_4).remainder(pos1) shouldBe pos1.remove(_4)
  }
}

class TestAlongPosition4D extends TestSlicePosition4D {

  "A Along[Position4D]" should "return a Position3D for the selected dimension" in {
    Along[_3, _4](_1).selected(pos1) shouldBe pos1.remove(_1)
    Along[_3, _4](_2).selected(pos1) shouldBe pos1.remove(_2)
    Along[_3, _4](_3).selected(pos1) shouldBe pos1.remove(_3)
    Along[_3, _4](_4).selected(pos1) shouldBe pos1.remove(_4)
  }

  it should "return a Position1D for the remainder" in {
    Along[_3, _4](_1).remainder(pos1) shouldBe Position(pos1(_1))
    Along[_3, _4](_2).remainder(pos1) shouldBe Position(pos1(_2))
    Along[_3, _4](_3).remainder(pos1) shouldBe Position(pos1(_3))
    Along[_3, _4](_4).remainder(pos1) shouldBe Position(pos1(_4))
  }
}

trait TestSlicePosition5D extends TestSlice {
  val pos1 = Position(5, "d", DateValue(dfmt.parse("2003-01-01"), DateCodec()), "bar", 3.1415)
  val pos2 = Position(-5, "w", DateValue(dfmt.parse("1997-01-01"), DateCodec()), "rab", -3.1415)
}

class TestOverPosition5D extends TestSlicePosition5D {

  "A Over[Position5D]" should "return a Position1D for the selected dimension" in {
    Over[_4, _5](_1).selected(pos1) shouldBe Position(pos1(_1))
    Over[_4, _5](_2).selected(pos1) shouldBe Position(pos1(_2))
    Over[_4, _5](_3).selected(pos1) shouldBe Position(pos1(_3))
    Over[_4, _5](_4).selected(pos1) shouldBe Position(pos1(_4))
    Over[_4, _5](_5).selected(pos1) shouldBe Position(pos1(_5))
  }

  it should "return a Position4D for the remainder" in {
    Over[_4, _5](_1).remainder(pos1) shouldBe pos1.remove(_1)
    Over[_4, _5](_2).remainder(pos1) shouldBe pos1.remove(_2)
    Over[_4, _5](_3).remainder(pos1) shouldBe pos1.remove(_3)
    Over[_4, _5](_4).remainder(pos1) shouldBe pos1.remove(_4)
    Over[_4, _5](_5).remainder(pos1) shouldBe pos1.remove(_5)
  }
}

class TestAlongPosition5D extends TestSlicePosition5D {

  "A Along[Position5D]" should "return a Position4D for the selected dimension" in {
    Along[_4, _5](_1).selected(pos1) shouldBe pos1.remove(_1)
    Along[_4, _5](_2).selected(pos1) shouldBe pos1.remove(_2)
    Along[_4, _5](_3).selected(pos1) shouldBe pos1.remove(_3)
    Along[_4, _5](_4).selected(pos1) shouldBe pos1.remove(_4)
    Along[_4, _5](_5).selected(pos1) shouldBe pos1.remove(_5)
  }

  it should "return a Position1D for the remainder" in {
    Along[_4, _5](_1).remainder(pos1) shouldBe Position(pos1(_1))
    Along[_4, _5](_2).remainder(pos1) shouldBe Position(pos1(_2))
    Along[_4, _5](_3).remainder(pos1) shouldBe Position(pos1(_3))
    Along[_4, _5](_4).remainder(pos1) shouldBe Position(pos1(_4))
    Along[_4, _5](_5).remainder(pos1) shouldBe Position(pos1(_5))
  }
}

