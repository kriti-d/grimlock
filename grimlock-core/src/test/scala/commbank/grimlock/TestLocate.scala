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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.{ ::, HNil }
import shapeless.nat.{ _0, _1 }

class TestRenameDimension extends TestGrimlock {
  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A RenameDimension" should "extract" in {
    val f = Locate.RenameDimension[Value[String] :: HNil, _0, Value[String], String](_0, _.toShortString + ".postfix")

    f(cell) shouldBe Option(Position("foo.postfix"))
  }
}

class TestRenameDimensionWithContent extends TestGrimlock {
  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A RenameDimensionWithContent" should "extract" in {
    val f = Locate.RenameDimensionWithContent[Value[String] :: HNil, _0, Value[String], String](
      _0,
      (crd, con) => con.toShortString + "<-" + crd.toShortString
    )

    f(cell) shouldBe Option(Position("1.0<-foo"))
  }
}

class TestAppendValue extends TestGrimlock {
  val cell = Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0))

  "A AppendValue" should "extract" in {
    val f = Locate.AppendValue[Value[String] :: HNil, Int](42)

    f(cell) shouldBe Option(Position("foo", 42))
  }
}

class TestPrependPairwiseSelectedStringToRemainder extends TestGrimlock {
  type P = Value[String] :: Value[String] :: Value[Int] :: HNil
  type S = Value[String] :: HNil
  type R = Value[String] :: Value[Int] :: HNil

  val left = Cell(Position("left", "abc", 123), Content(ContinuousSchema[Double](), 1.0))
  val right = Cell(Position("right", "def", 456), Content(ContinuousSchema[Double](), 2.0))

  "A PrependPairwiseSelectedToRemainder" should "extract with all" in {
    val f = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Over(_0), "%1$s-%2$s", false, "|")
    val g = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Over(_0), "%1$s-%2$s", false, "|")

    f(left, right) shouldBe None
    g(right, right) shouldBe Option(Position("right-right", "def", 456))
  }

  it should "extract with non-all" in {
    val f = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Over(_0), "%1$s-%2$s", true, "|")
    val g = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Over(_0), "%1$s-%2$s", true, "|")

    f(left, right) shouldBe Option(Position("left-right", "abc", 123))
    g(right, left) shouldBe Option(Position("right-left", "def", 456))
  }
}

class TestAppendRemainderDimension extends TestGrimlock {
  type S = Value[String] :: HNil
  type R = Value[String] :: Value[Int] :: HNil

  val sel = Position("foo")
  val rem = Position("abc", 123)

  "A AppendRemainderDimension" should "extract" in {
    val f = Locate.AppendRemainderDimension[S, R, _0, Value[String]](_0)
    val g = Locate.AppendRemainderDimension[S, R, _1, Value[Int]](_1)

    f(sel, rem) shouldBe Option(Position("foo", "abc"))
    g(sel, rem) shouldBe Option(Position("foo", 123))
  }
}

class TestAppendRemainderString extends TestGrimlock {
  type S = Value[String] :: HNil
  type R = Value[String] :: Value[Int] :: HNil

  val sel = Position("foo")
  val rem = Position("abc", 123)

  "A AppendRemainderString" should "extract" in {
    val f = Locate.AppendRemainderString[S, R](":")

    f(sel, rem) shouldBe Option(Position("foo", "abc:123"))
  }
}

class TestAppendPairwiseString extends TestGrimlock {
  type S = Value[String] :: HNil
  type R = Value[String] :: Value[Int] :: HNil

  val sel = Position("foo")
  val curr = Position("abc", 123)
  val prev = Position("def", 456)

  "A AppendPairwiseRemainderString" should "extract" in {
    val f = Locate.AppendPairwiseRemainderString[S, R]("g(%2$s, %1$s)", ":")

    f(sel, curr, prev) shouldBe Option(Position("foo", "g(abc:123, def:456)"))
  }
}

class TestAppendContentString extends TestGrimlock {
  val pos = Position("foo")
  val con = Content(DiscreteSchema[Long](), 42L)

  "A AppendContentString" should "extract" in {
    val f = Locate.AppendContentString[Value[String] :: HNil]

    f(pos, con) shouldBe Option(Position("foo", "42"))
  }
}

class TestAppendDimensionAndContentString extends TestGrimlock {
  val pos = Position("foo")
  val con = Content(DiscreteSchema[Long](), 42L)

  "A AppendDimensionWithContent" should "extract" in {
    val f = Locate.AppendDimensionWithContent[Value[String] :: HNil, _0, Value[String], String](
      _0,
      (crd, con) => con.toShortString + "!=" + crd.toShortString
    )

    f(pos, con) shouldBe Option(Position("foo", "42!=foo"))
  }
}

