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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _1, _2, _3 }

class TestRenameDimension extends TestGrimlock {

  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A RenameDimension" should "extract" in {
    val f = Locate.RenameDimension[_1, _1](_1, "%1$s.postfix")
    f(cell) shouldBe Option(Position("foo.postfix"))
  }
}

class TestRenameDimensionWithContent extends TestGrimlock {

  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A RenameDimensionWithContent" should "extract" in {
    val f = Locate.RenameDimensionWithContent[_1, _1](_1, "%2$s<-%1$s")
    f(cell) shouldBe Option(Position("1.0<-foo"))
  }
}

class TestAppendValue extends TestGrimlock {

  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A AppendValue" should "extract" in {
    val f = Locate.AppendValue[_1](42)
    f(cell) shouldBe Option(Position("foo", 42))
  }
}

class TestPrependPairwiseSelectedStringToRemainder extends TestGrimlock {

  val left = Cell(Position("left", "abc", 123), Content(ContinuousSchema[Double](), 1.0))
  val right = Cell(Position("right", "def", 456), Content(ContinuousSchema[Double](), 2.0))

  "A PrependPairwiseSelectedToRemainder" should "extract with all" in {
    val f = Locate.PrependPairwiseSelectedStringToRemainder[_3](Over(_1), "%1$s-%2$s", false)
    f(left, right) shouldBe None
    val g = Locate.PrependPairwiseSelectedStringToRemainder[_3](Over(_1), "%1$s-%2$s", false)
    g(right, right) shouldBe Option(Position("right-right", "def", 456))
  }

  it should "extract with non-all" in {
    val f = Locate.PrependPairwiseSelectedStringToRemainder[_3](Over(_1), "%1$s-%2$s", true)
    f(left, right) shouldBe Option(Position("left-right", "abc", 123))
    val g = Locate.PrependPairwiseSelectedStringToRemainder[_3](Over(_1), "%1$s-%2$s", true)
    g(right, left) shouldBe Option(Position("right-left", "def", 456))
  }
}

class TestAppendRemainderDimension extends TestGrimlock {

  val sel = Position("foo")
  val rem = Position("abc", 123)

  "A AppendRemainderDimension" should "extract" in {
    val f = Locate.AppendRemainderDimension[_1, _2, _1](_1)
    f(sel, rem) shouldBe Option(Position("foo", "abc"))
    val g = Locate.AppendRemainderDimension[_1, _2, _2](_2)
    g(sel, rem) shouldBe Option(Position("foo", 123))
  }
}

class TestAppendRemainderString extends TestGrimlock {

  val sel = Position("foo")
  val rem = Position("abc", 123)

  "A AppendRemainderString" should "extract" in {
    val f = Locate.AppendRemainderString[_1, _2](":")
    f(sel, rem) shouldBe Option(Position("foo", "abc:123"))
  }
}

class TestAppendPairwiseString extends TestGrimlock {

  val sel = Position("foo")
  val curr = Position("abc", 123)
  val prev = Position("def", 456)

  "A AppendPairwiseString" should "extract" in {
    val f = Locate.AppendPairwiseString[_1, _2]("g(%2$s, %1$s)", ":")
    f(sel, curr, prev) shouldBe Option(Position("foo", "g(abc:123, def:456)"))
  }
}

class TestAppendDoubleString extends TestGrimlock {

  val pos = Position("foo")

  "A AppendDoubleString" should "extract" in {
    val f = Locate.AppendDoubleString[_1]("value=%1$s")
    f(pos, 42) shouldBe Option(Position("foo", "value=42.0"))
  }
}

class TestAppendContentString extends TestGrimlock {

  val pos = Position("foo")
  val con = Content(DiscreteSchema[Long](), 42)

  "A AppendContentString" should "extract" in {
    val f = Locate.AppendContentString[_1]()
    f(pos, con) shouldBe Option(Position("foo", "42"))
  }
}

class TestAppendDimensionAndContentString extends TestGrimlock {

  val pos = Position("foo")
  val con = Content(DiscreteSchema[Long](), 42)

  "A AppendDimensionAndContentString" should "extract" in {
    val f = Locate.AppendDimensionAndContentString[_1, _1](_1, "%2$s!=%1$s")
    f(pos, con) shouldBe Option(Position("foo", "42!=foo"))
  }
}

