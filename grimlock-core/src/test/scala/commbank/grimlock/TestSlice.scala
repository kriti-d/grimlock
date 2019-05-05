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

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import java.util.Date

import shapeless.{ ::, HNil }
import shapeless.nat.{ _0, _1, _2 }

trait TestSlice extends TestGrimlock {
  implicit def toDate(date: Date): DateValue = DateValue(date)

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val con1 = Content(ContinuousSchema[Long](), 1L)
  val con2 = Content(ContinuousSchema[Long](), 2L)
}

trait TestSlicePosition1D extends TestSlice {
  type P = Value[Int] :: HNil

  val pos1 = Position(1)
  val pos2 = Position(-1)
}

class TestOverPosition extends TestSlicePosition1D {
  "A Over[Position1D]" should "return a Position1D for the selected dimension" in {
    Over[P, _0, Value[Int], HNil](_0).selected(pos1) shouldBe Position(pos1(_0))
  }

  it should "return a Position0D for the remainder" in {
    Over[P, _0, Value[Int], HNil](_0).remainder(pos1) shouldBe pos1.remove(_0)
  }
}

class TestAlongPosition1D extends TestSlicePosition1D {
  "A Along[Position1D]" should "return a Position0D for the selected dimension" in {
    Along[P, _0, Value[Int], HNil](_0).selected(pos1) shouldBe pos1.remove(_0)
  }

  it should "return a Position1D for the remainder" in {
    Along[P, _0, Value[Int], HNil](_0).remainder(pos1) shouldBe Position(pos1(_0))
  }
}

trait TestSlicePosition2D extends TestSlice {
  type P = Value[Int] :: Value[String] :: HNil

  val pos1 = Position(2, "a")
  val pos2 = Position(-2, "z")
}

class TestOverPosition2D extends TestSlicePosition2D {
  "A Over[Position2D]" should "return a Position1D for the selected dimension" in {
    Over[P, _0, Value[Int], Value[String] :: HNil](_0).selected(pos1) shouldBe Position(pos1(_0))
    Over[P, _1, Value[String], Value[Int] :: HNil](_1).selected(pos1) shouldBe Position(pos1(_1))
  }

  it should "return a Position1D for the remainder" in {
    Over[P, _0, Value[Int], Value[String] :: HNil](_0).remainder(pos1) shouldBe pos1.remove(_0)
    Over[P, _1, Value[String], Value[Int] :: HNil](_1).remainder(pos1) shouldBe pos1.remove(_1)
  }
}

class TestAlongPosition2D extends TestSlicePosition2D {
  "A Along[Position2D]" should "return a Position1D for the selected dimension" in {
    Along[P, _0, Value[Int], Value[String] :: HNil](_0).selected(pos1) shouldBe pos1.remove(_0)
    Along[P, _1, Value[String], Value[Int] :: HNil](_1).selected(pos1) shouldBe pos1.remove(_1)
  }

  it should "return a Position1D for the remainder" in {
    Along[P, _0, Value[Int], Value[String] :: HNil](_0).remainder(pos1) shouldBe Position(pos1(_0))
    Along[P, _1, Value[String], Value[Int] :: HNil](_1).remainder(pos1) shouldBe Position(pos1(_1))
  }
}

trait TestSlicePosition3D extends TestSlice {
  type P = Value[Int] :: Value[String] :: Value[Date] :: HNil

  val pos1 = Position(3, "b", dfmt.parse("2001-01-01"))
  val pos2 = Position(-3, "y", dfmt.parse("1999-01-01"))
}

class TestOverPosition3D extends TestSlicePosition3D {
  "A Over[Position3D]" should "return a Position1D for the selected dimension" in {
    Over[P, _0, Value[Int], Value[String] :: Value[Date] :: HNil](_0).selected(pos1) shouldBe Position(pos1(_0))
    Over[P, _1, Value[String], Value[Int] :: Value[Date] :: HNil](_1).selected(pos1) shouldBe Position(pos1(_1))
    Over[P, _2, Value[Date], Value[Int] :: Value[String] :: HNil](_2).selected(pos1) shouldBe Position(pos1(_2))
  }

  it should "return a Position2D for the remainder" in {
    Over[P, _0, Value[Int], Value[String] :: Value[Date] :: HNil](_0).remainder(pos1) shouldBe pos1.remove(_0)
    Over[P, _1, Value[String], Value[Int] :: Value[Date] :: HNil](_1).remainder(pos1) shouldBe pos1.remove(_1)
    Over[P, _2, Value[Date], Value[Int] :: Value[String] :: HNil](_2).remainder(pos1) shouldBe pos1.remove(_2)
  }
}

class TestAlongPosition3D extends TestSlicePosition3D {
  "A Along[Position3D]" should "return a Position2D for the selected dimension" in {
    Along[P, _0, Value[Int], Value[String] :: Value[Date] :: HNil](_0).selected(pos1) shouldBe pos1.remove(_0)
    Along[P, _1, Value[String], Value[Int] :: Value[Date] :: HNil](_1).selected(pos1) shouldBe pos1.remove(_1)
    Along[P, _2, Value[Date], Value[Int] :: Value[String] :: HNil](_2).selected(pos1) shouldBe pos1.remove(_2)
  }

  it should "return a Position1D for the remainder" in {
    Along[P, _0, Value[Int], Value[String] :: Value[Date] :: HNil](_0).remainder(pos1) shouldBe Position(pos1(_0))
    Along[P, _1, Value[String], Value[Int] :: Value[Date] :: HNil](_1).remainder(pos1) shouldBe Position(pos1(_1))
    Along[P, _2, Value[Date], Value[Int] :: Value[String] :: HNil](_2).remainder(pos1) shouldBe Position(pos1(_2))
  }
}

