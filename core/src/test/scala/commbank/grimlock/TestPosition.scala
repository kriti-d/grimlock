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
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.position.Position._

import commbank.grimlock.scalding.environment.Context._

import commbank.grimlock.spark.environment.Context._

import scala.collection.immutable.ListSet

import shapeless.nat.{ _1, _2, _3, _4, _5 }
import shapeless.syntax.sized._

trait TestPosition extends TestGrimlock {
  def merge(i: Value, d: Value): Value = i.toShortString + "|" + d.toShortString
}

class TestPosition0D extends TestPosition {

  val pos = Position()

  "A Position0D" should "return its short string" in {
    pos.toShortString("|") shouldBe ""
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List()
  }

  it should "compare" in {
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz")) should be < 0
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position(123)
  }

  it should "append" in {
    pos.append(123) shouldBe Position(123)
  }
}

class TestPosition1D extends TestPosition {

  val pos = Position("foo")

  "A Position1D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"))
  }

  it should "return its coordinates" in {
    pos(_1) shouldBe StringValue("foo")
  }

  it should "compare" in {
    pos.compare(Position("abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz")) should be < 0
    pos.compare(Position("abc", "xyz")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position(123)) }
  }

  it should "be updated" in {
    pos.update(_1, 123) shouldBe Position(123)
  }

  it should "remove" in {
    pos.remove(_1) shouldBe Position()
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position(123, "foo")
  }

  it should "append" in {
    pos.append(123) shouldBe Position("foo", 123)
  }
}

class TestPosition2D extends TestPosition {

  val pos = Position("foo", 123)

  "A Position2D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123))
  }

  it should "return its coordinates" in {
    pos(_1) shouldBe StringValue("foo")
    pos(_2) shouldBe LongValue(123)
  }

  it should "compare" in {
    pos.compare(Position("abc", 456)) should be > 0
    pos.compare(Position("foo", 1)) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1)) should be < 0
    pos.compare(Position("foo", 789)) should be < 0
    pos.compare(Position("xyz")) should be > 0
    pos.compare(Position("abc", 1, "bar")) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position(123, 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("xyz", "foo")) }
  }

  it should "be updated" in {
    pos.update(_1, 123) shouldBe Position(123, 123)
    pos.update(_2, "xyz") shouldBe Position("foo", "xyz")
  }

  it should "permute" in {
    pos.permute(ListSet(1, 2).sized(2).get) shouldBe pos
    pos.permute(ListSet(2, 1).sized(2).get) shouldBe Position(123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(3).sized(2).get) }
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(1, 2, 3).sized(2).get) }
  }

  it should "remove" in {
    pos.remove(_1) shouldBe Position(123)
    pos.remove(_2) shouldBe Position("foo")
  }

  it should "melt" in {
    pos.melt(_1, _2, merge) shouldBe Position("123|foo")
    pos.melt(_2, _1, merge) shouldBe Position("foo|123")
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position(123, "foo", 123)
  }

  it should "append" in {
    pos.append(123) shouldBe Position("foo", 123, 123)
  }
}

class TestPosition3D extends TestPosition {

  val pos = Position("foo", 123, "bar")

  "A Position3D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123), StringValue("bar"))
  }

  it should "return its coordinates" in {
    pos(_1) shouldBe StringValue("foo")
    pos(_2) shouldBe LongValue(123)
    pos(_3) shouldBe StringValue("bar")
  }

  it should "compare" in {
    pos.compare(Position("abc", 456, "xyz")) should be > 0
    pos.compare(Position("foo", 1, "xyz")) should be > 0
    pos.compare(Position("foo", 123, "abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1, "abc")) should be < 0
    pos.compare(Position("foo", 789, "abc")) should be < 0
    pos.compare(Position("foo", 123, "xyz")) should be < 0
    pos.compare(Position("xyz", 456)) should be > 0
    pos.compare(Position("abc", 1, "abc", 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position(123, 123, "bar")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", "123", "bar")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, 123)) }
  }

  it should "be updated" in {
    pos.update(_1, 123) shouldBe Position(123, 123, "bar")
    pos.update(_2, "xyz") shouldBe Position("foo", "xyz", "bar")
    pos.update(_3, 456) shouldBe Position("foo", 123, 456)
  }

  it should "permute" in {
    pos.permute(ListSet(1, 2, 3).sized(3).get) shouldBe pos
    pos.permute(ListSet(1, 3, 2).sized(3).get) shouldBe Position("foo", "bar", 123)
    pos.permute(ListSet(2, 1, 3).sized(3).get) shouldBe Position(123, "foo", "bar")
    pos.permute(ListSet(2, 3, 1).sized(3).get) shouldBe Position("bar", "foo", 123)
    pos.permute(ListSet(3, 1, 2).sized(3).get) shouldBe Position(123, "bar", "foo")
    pos.permute(ListSet(3, 2, 1).sized(3).get) shouldBe Position("bar", 123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(4).sized(3).get) }
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(1, 2, 3, 4).sized(3).get) }
  }

  it should "remove" in {
    pos.remove(_1) shouldBe Position(123, "bar")
    pos.remove(_2) shouldBe Position("foo", "bar")
    pos.remove(_3) shouldBe Position("foo", 123)
  }

  it should "melt" in {
    pos.melt(_1, _2, merge) shouldBe Position("123|foo", "bar")
    pos.melt(_1, _3, merge) shouldBe Position(123, "bar|foo")

    pos.melt(_2, _1, merge) shouldBe Position("foo|123", "bar")
    pos.melt(_2, _3, merge) shouldBe Position("foo", "bar|123")

    pos.melt(_3, _1, merge) shouldBe Position("foo|bar", 123)
    pos.melt(_3, _2, merge) shouldBe Position("foo", "123|bar")
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position(123, "foo", 123, "bar")
  }

  it should "append" in {
    pos.append(123) shouldBe Position("foo", 123, "bar", 123)
  }
}

class TestPosition4D extends TestPosition {

  val pos = Position("foo", 123, "bar", 456)

  "A Position4D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar|456"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(StringValue("foo"), LongValue(123), StringValue("bar"), LongValue(456))
  }

  it should "return its coordinates" in {
    pos(_1) shouldBe StringValue("foo")
    pos(_2) shouldBe LongValue(123)
    pos(_3) shouldBe StringValue("bar")
    pos(_4) shouldBe LongValue(456)
  }

  it should "compare" in {
    pos.compare(Position("abc", 456, "xyz", 789)) should be > 0
    pos.compare(Position("foo", 1, "xyz", 789)) should be > 0
    pos.compare(Position("foo", 123, "abc", 789)) should be > 0
    pos.compare(Position("foo", 123, "bar", 1)) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1, "abc", 1)) should be < 0
    pos.compare(Position("foo", 789, "abc", 1)) should be < 0
    pos.compare(Position("foo", 123, "xyz", 1)) should be < 0
    pos.compare(Position("foo", 123, "bar", 789)) should be < 0
    pos.compare(Position("xyz", 456, "xyz")) should be > 0
    pos.compare(Position("abc", 1, "abc", 1, 1)) should be < 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position(123, 123, "bar", 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", "123", "bar", 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, 123, 456)) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, "bar", "456")) }
  }

  it should "be updated" in {
    pos.update(_1, 123) shouldBe Position(123, 123, "bar", 456)
    pos.update(_2, "xyz") shouldBe Position("foo", "xyz", "bar", 456)
    pos.update(_3, 456) shouldBe Position("foo", 123, 456, 456)
    pos.update(_4, "abc") shouldBe Position("foo", 123, "bar", "abc")
  }

  it should "permute" in {
    pos.permute(ListSet(1, 2, 3, 4).sized(4).get) shouldBe pos
    pos.permute(ListSet(1, 2, 4, 3).sized(4).get) shouldBe Position("foo", 123, 456, "bar")
    pos.permute(ListSet(1, 3, 2, 4).sized(4).get) shouldBe Position("foo", "bar", 123, 456)
    pos.permute(ListSet(1, 3, 4, 2).sized(4).get) shouldBe Position("foo", 456, 123, "bar")
    pos.permute(ListSet(1, 4, 2, 3).sized(4).get) shouldBe Position("foo", "bar", 456, 123)
    pos.permute(ListSet(1, 4, 3, 2).sized(4).get) shouldBe Position("foo", 456, "bar", 123)

    pos.permute(ListSet(2, 1, 3, 4).sized(4).get) shouldBe Position(123, "foo", "bar", 456)
    pos.permute(ListSet(2, 1, 4, 3).sized(4).get) shouldBe Position(123, "foo", 456, "bar")
    pos.permute(ListSet(2, 3, 1, 4).sized(4).get) shouldBe Position("bar", "foo", 123, 456)
    pos.permute(ListSet(2, 3, 4, 1).sized(4).get) shouldBe Position(456, "foo", 123, "bar")
    pos.permute(ListSet(2, 4, 1, 3).sized(4).get) shouldBe Position("bar", "foo", 456, 123)
    pos.permute(ListSet(2, 4, 3, 1).sized(4).get) shouldBe Position(456, "foo", "bar", 123)

    pos.permute(ListSet(3, 1, 2, 4).sized(4).get) shouldBe Position(123, "bar", "foo", 456)
    pos.permute(ListSet(3, 1, 4, 2).sized(4).get) shouldBe Position(123, 456, "foo", "bar")
    pos.permute(ListSet(3, 2, 1, 4).sized(4).get) shouldBe Position("bar", 123, "foo", 456)
    pos.permute(ListSet(3, 2, 4, 1).sized(4).get) shouldBe Position(456, 123, "foo", "bar")
    pos.permute(ListSet(3, 4, 1, 2).sized(4).get) shouldBe Position("bar", 456, "foo", 123)
    pos.permute(ListSet(3, 4, 2, 1).sized(4).get) shouldBe Position(456, "bar", "foo", 123)

    pos.permute(ListSet(4, 1, 2, 3).sized(4).get) shouldBe Position(123, "bar", 456, "foo")
    pos.permute(ListSet(4, 1, 3, 2).sized(4).get) shouldBe Position(123, 456, "bar", "foo")
    pos.permute(ListSet(4, 2, 1, 3).sized(4).get) shouldBe Position("bar", 123, 456, "foo")
    pos.permute(ListSet(4, 2, 3, 1).sized(4).get) shouldBe Position(456, 123, "bar", "foo")
    pos.permute(ListSet(4, 3, 1, 2).sized(4).get) shouldBe Position("bar", 456, 123, "foo")
    pos.permute(ListSet(4, 3, 2, 1).sized(4).get) shouldBe Position(456, "bar", 123, "foo")
  }

  it should "throw an exception for an invalid permute dimension" in {
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(5).sized(4).get) }
    a [Exception] shouldBe thrownBy { pos.permute(ListSet(1, 2, 3, 4, 5).sized(4).get) }
  }

  it should "remove" in {
    pos.remove(_1) shouldBe Position(123, "bar", 456)
    pos.remove(_2) shouldBe Position("foo", "bar", 456)
    pos.remove(_3) shouldBe Position("foo", 123, 456)
    pos.remove(_4) shouldBe Position("foo", 123, "bar")
  }

  it should "melt" in {
    pos.melt(_1, _2, merge) shouldBe Position("123|foo", "bar", 456)
    pos.melt(_1, _3, merge) shouldBe Position(123, "bar|foo", 456)
    pos.melt(_1, _4, merge) shouldBe Position(123, "bar", "456|foo")

    pos.melt(_2, _1, merge) shouldBe Position("foo|123", "bar", 456)
    pos.melt(_2, _3, merge) shouldBe Position("foo", "bar|123", 456)
    pos.melt(_2, _4, merge) shouldBe Position("foo", "bar", "456|123")

    pos.melt(_3, _1, merge) shouldBe Position("foo|bar", 123, 456)
    pos.melt(_3, _2, merge) shouldBe Position("foo", "123|bar", 456)
    pos.melt(_3, _4, merge) shouldBe Position("foo", 123, "456|bar")

    pos.melt(_4, _1, merge) shouldBe Position("foo|456", 123, "bar")
    pos.melt(_4, _2, merge) shouldBe Position("foo", "123|456", "bar")
    pos.melt(_4, _3, merge) shouldBe Position("foo", 123, "bar|456")
  }

  it should "prepend" in {
    pos.prepend(123) shouldBe Position(123, "foo", 123, "bar", 456)
  }

  it should "append" in {
    pos.append(123) shouldBe Position("foo", 123, "bar", 456, 123)
  }
}

class TestPosition5D extends TestPosition {

  val pos = Position("foo", 123, "bar", 456, "baz")

  "A Position5D" should "return its short string" in {
    pos.toShortString("|") shouldBe "foo|123|bar|456|baz"
  }

  it should "have coordinates" in {
    pos.coordinates shouldBe List(
      StringValue("foo"),
      LongValue(123),
      StringValue("bar"),
      LongValue(456),
      StringValue("baz")
    )
  }

  it should "return its coordinates" in {
    pos(_1) shouldBe StringValue("foo")
    pos(_2) shouldBe LongValue(123)
    pos(_3) shouldBe StringValue("bar")
    pos(_4) shouldBe LongValue(456)
    pos(_5) shouldBe StringValue("baz")
  }

  it should "compare" in {
    pos.compare(Position("abc", 456, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position("foo", 1, "xyz", 789, "xyz")) should be > 0
    pos.compare(Position("foo", 123, "abc", 789, "xyz")) should be > 0
    pos.compare(Position("foo", 123, "bar", 1, "xyz")) should be > 0
    pos.compare(Position("foo", 123, "bar", 456, "abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1, "abc", 1, "abc")) should be < 0
    pos.compare(Position("foo", 789, "abc", 1, "abc")) should be < 0
    pos.compare(Position("foo", 123, "xyz", 1, "abc")) should be < 0
    pos.compare(Position("foo", 123, "bar", 789, "abc")) should be < 0
    pos.compare(Position("foo", 123, "bar", 456, "xyz")) should be < 0
    pos.compare(Position("xyz", 456, "xyz", 789)) should be > 0
  }

  it should "throw an exception for an invalid value" in {
    a [Exception] shouldBe thrownBy { pos.compare(Position(123, 123, "bar", 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", "123", "bar", 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, 123, 456, "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, "bar", "456", "baz")) }
    a [Exception] shouldBe thrownBy { pos.compare(Position("foo", 123, "bar", 456, 123)) }
  }

  it should "be updated" in {
    pos.update(_1, 123) shouldBe Position(123, 123, "bar", 456, "baz")
    pos.update(_2, "xyz") shouldBe Position("foo", "xyz", "bar", 456, "baz")
    pos.update(_3, 456) shouldBe Position("foo", 123, 456, 456, "baz")
    pos.update(_4, "abc") shouldBe Position("foo", 123, "bar", "abc", "baz")
    pos.update(_5, 789) shouldBe Position("foo", 123, "bar", 456, 789)
  }

  it should "permute" in {
    pos.permute(ListSet(1, 2, 3, 4, 5).sized(5).get) shouldBe Position("foo", 123, "bar", 456, "baz")
    pos.permute(ListSet(1, 2, 3, 5, 4).sized(5).get) shouldBe Position("foo", 123, "bar", "baz", 456)
    pos.permute(ListSet(1, 2, 4, 3, 5).sized(5).get) shouldBe Position("foo", 123, 456, "bar", "baz")
    pos.permute(ListSet(1, 2, 4, 5, 3).sized(5).get) shouldBe Position("foo", 123, "baz", "bar", 456)
    pos.permute(ListSet(1, 2, 5, 3, 4).sized(5).get) shouldBe Position("foo", 123, 456, "baz", "bar")
    pos.permute(ListSet(1, 2, 5, 4, 3).sized(5).get) shouldBe Position("foo", 123, "baz", 456, "bar")
    pos.permute(ListSet(1, 3, 2, 4, 5).sized(5).get) shouldBe Position("foo", "bar", 123, 456, "baz")
    pos.permute(ListSet(1, 3, 2, 5, 4).sized(5).get) shouldBe Position("foo", "bar", 123, "baz", 456)
    pos.permute(ListSet(1, 3, 4, 2, 5).sized(5).get) shouldBe Position("foo", 456, 123, "bar", "baz")
    pos.permute(ListSet(1, 3, 4, 5, 2).sized(5).get) shouldBe Position("foo", "baz", 123, "bar", 456)
    pos.permute(ListSet(1, 3, 5, 2, 4).sized(5).get) shouldBe Position("foo", 456, 123, "baz", "bar")
    pos.permute(ListSet(1, 3, 5, 4, 2).sized(5).get) shouldBe Position("foo", "baz", 123, 456, "bar")
    pos.permute(ListSet(1, 4, 2, 3, 5).sized(5).get) shouldBe Position("foo", "bar", 456, 123, "baz")
    pos.permute(ListSet(1, 4, 2, 5, 3).sized(5).get) shouldBe Position("foo", "bar", "baz", 123, 456)
    pos.permute(ListSet(1, 4, 3, 2, 5).sized(5).get) shouldBe Position("foo", 456, "bar", 123, "baz")
    pos.permute(ListSet(1, 4, 3, 5, 2).sized(5).get) shouldBe Position("foo", "baz", "bar", 123, 456)
    pos.permute(ListSet(1, 4, 5, 2, 3).sized(5).get) shouldBe Position("foo", 456, "baz", 123, "bar")
    pos.permute(ListSet(1, 4, 5, 3, 2).sized(5).get) shouldBe Position("foo", "baz", 456, 123, "bar")
    pos.permute(ListSet(1, 5, 2, 3, 4).sized(5).get) shouldBe Position("foo", "bar", 456, "baz", 123)
    pos.permute(ListSet(1, 5, 2, 4, 3).sized(5).get) shouldBe Position("foo", "bar", "baz", 456, 123)
    pos.permute(ListSet(1, 5, 3, 2, 4).sized(5).get) shouldBe Position("foo", 456, "bar", "baz", 123)
    pos.permute(ListSet(1, 5, 3, 4, 2).sized(5).get) shouldBe Position("foo", "baz", "bar", 456, 123)
    pos.permute(ListSet(1, 5, 4, 2, 3).sized(5).get) shouldBe Position("foo", 456, "baz", "bar", 123)
    pos.permute(ListSet(1, 5, 4, 3, 2).sized(5).get) shouldBe Position("foo", "baz", 456, "bar", 123)

    pos.permute(ListSet(2, 1, 3, 4, 5).sized(5).get) shouldBe Position(123, "foo", "bar", 456, "baz")
    pos.permute(ListSet(2, 1, 3, 5, 4).sized(5).get) shouldBe Position(123, "foo", "bar", "baz", 456)
    pos.permute(ListSet(2, 1, 4, 3, 5).sized(5).get) shouldBe Position(123, "foo", 456, "bar", "baz")
    pos.permute(ListSet(2, 1, 4, 5, 3).sized(5).get) shouldBe Position(123, "foo", "baz", "bar", 456)
    pos.permute(ListSet(2, 1, 5, 3, 4).sized(5).get) shouldBe Position(123, "foo", 456, "baz", "bar")
    pos.permute(ListSet(2, 1, 5, 4, 3).sized(5).get) shouldBe Position(123, "foo", "baz", 456, "bar")
    pos.permute(ListSet(2, 3, 1, 4, 5).sized(5).get) shouldBe Position("bar", "foo", 123, 456, "baz")
    pos.permute(ListSet(2, 3, 1, 5, 4).sized(5).get) shouldBe Position("bar", "foo", 123, "baz", 456)
    pos.permute(ListSet(2, 3, 4, 1, 5).sized(5).get) shouldBe Position(456, "foo", 123, "bar", "baz")
    pos.permute(ListSet(2, 3, 4, 5, 1).sized(5).get) shouldBe Position("baz", "foo", 123, "bar", 456)
    pos.permute(ListSet(2, 3, 5, 1, 4).sized(5).get) shouldBe Position(456, "foo", 123, "baz", "bar")
    pos.permute(ListSet(2, 3, 5, 4, 1).sized(5).get) shouldBe Position("baz", "foo", 123, 456, "bar")
    pos.permute(ListSet(2, 4, 1, 3, 5).sized(5).get) shouldBe Position("bar", "foo", 456, 123, "baz")
    pos.permute(ListSet(2, 4, 1, 5, 3).sized(5).get) shouldBe Position("bar", "foo", "baz", 123, 456)
    pos.permute(ListSet(2, 4, 3, 1, 5).sized(5).get) shouldBe Position(456, "foo", "bar", 123, "baz")
    pos.permute(ListSet(2, 4, 3, 5, 1).sized(5).get) shouldBe Position("baz", "foo", "bar", 123, 456)
    pos.permute(ListSet(2, 4, 5, 1, 3).sized(5).get) shouldBe Position(456, "foo", "baz", 123, "bar")
    pos.permute(ListSet(2, 4, 5, 3, 1).sized(5).get) shouldBe Position("baz", "foo", 456, 123, "bar")
    pos.permute(ListSet(2, 5, 1, 3, 4).sized(5).get) shouldBe Position("bar", "foo", 456, "baz", 123)
    pos.permute(ListSet(2, 5, 1, 4, 3).sized(5).get) shouldBe Position("bar", "foo", "baz", 456, 123)
    pos.permute(ListSet(2, 5, 3, 1, 4).sized(5).get) shouldBe Position(456, "foo", "bar", "baz", 123)
    pos.permute(ListSet(2, 5, 3, 4, 1).sized(5).get) shouldBe Position("baz", "foo", "bar", 456, 123)
    pos.permute(ListSet(2, 5, 4, 1, 3).sized(5).get) shouldBe Position(456, "foo", "baz", "bar", 123)
    pos.permute(ListSet(2, 5, 4, 3, 1).sized(5).get) shouldBe Position("baz", "foo", 456, "bar", 123)

    pos.permute(ListSet(3, 1, 2, 4, 5).sized(5).get) shouldBe Position(123, "bar", "foo", 456, "baz")
    pos.permute(ListSet(3, 1, 2, 5, 4).sized(5).get) shouldBe Position(123, "bar", "foo", "baz", 456)
    pos.permute(ListSet(3, 1, 4, 2, 5).sized(5).get) shouldBe Position(123, 456, "foo", "bar", "baz")
    pos.permute(ListSet(3, 1, 4, 5, 2).sized(5).get) shouldBe Position(123, "baz", "foo", "bar", 456)
    pos.permute(ListSet(3, 1, 5, 2, 4).sized(5).get) shouldBe Position(123, 456, "foo", "baz", "bar")
    pos.permute(ListSet(3, 1, 5, 4, 2).sized(5).get) shouldBe Position(123, "baz", "foo", 456, "bar")
    pos.permute(ListSet(3, 2, 1, 4, 5).sized(5).get) shouldBe Position("bar", 123, "foo", 456, "baz")
    pos.permute(ListSet(3, 2, 1, 5, 4).sized(5).get) shouldBe Position("bar", 123, "foo", "baz", 456)
    pos.permute(ListSet(3, 2, 4, 1, 5).sized(5).get) shouldBe Position(456, 123, "foo", "bar", "baz")
    pos.permute(ListSet(3, 2, 4, 5, 1).sized(5).get) shouldBe Position("baz", 123, "foo", "bar", 456)
    pos.permute(ListSet(3, 2, 5, 1, 4).sized(5).get) shouldBe Position(456, 123, "foo", "baz", "bar")
    pos.permute(ListSet(3, 2, 5, 4, 1).sized(5).get) shouldBe Position("baz", 123, "foo", 456, "bar")
    pos.permute(ListSet(3, 4, 1, 2, 5).sized(5).get) shouldBe Position("bar", 456, "foo", 123, "baz")
    pos.permute(ListSet(3, 4, 1, 5, 2).sized(5).get) shouldBe Position("bar", "baz", "foo", 123, 456)
    pos.permute(ListSet(3, 4, 2, 1, 5).sized(5).get) shouldBe Position(456, "bar", "foo", 123, "baz")
    pos.permute(ListSet(3, 4, 2, 5, 1).sized(5).get) shouldBe Position("baz", "bar", "foo", 123, 456)
    pos.permute(ListSet(3, 4, 5, 1, 2).sized(5).get) shouldBe Position(456, "baz", "foo", 123, "bar")
    pos.permute(ListSet(3, 4, 5, 2, 1).sized(5).get) shouldBe Position("baz", 456, "foo", 123, "bar")
    pos.permute(ListSet(3, 5, 1, 2, 4).sized(5).get) shouldBe Position("bar", 456, "foo", "baz", 123)
    pos.permute(ListSet(3, 5, 1, 4, 2).sized(5).get) shouldBe Position("bar", "baz", "foo", 456, 123)
    pos.permute(ListSet(3, 5, 2, 1, 4).sized(5).get) shouldBe Position(456, "bar", "foo", "baz", 123)
    pos.permute(ListSet(3, 5, 2, 4, 1).sized(5).get) shouldBe Position("baz", "bar", "foo", 456, 123)
    pos.permute(ListSet(3, 5, 4, 1, 2).sized(5).get) shouldBe Position(456, "baz", "foo", "bar", 123)
    pos.permute(ListSet(3, 5, 4, 2, 1).sized(5).get) shouldBe Position("baz", 456, "foo", "bar", 123)

    pos.permute(ListSet(4, 1, 2, 3, 5).sized(5).get) shouldBe Position(123, "bar", 456, "foo", "baz")
    pos.permute(ListSet(4, 1, 2, 5, 3).sized(5).get) shouldBe Position(123, "bar", "baz", "foo", 456)
    pos.permute(ListSet(4, 1, 3, 2, 5).sized(5).get) shouldBe Position(123, 456, "bar", "foo", "baz")
    pos.permute(ListSet(4, 1, 3, 5, 2).sized(5).get) shouldBe Position(123, "baz", "bar", "foo", 456)
    pos.permute(ListSet(4, 1, 5, 2, 3).sized(5).get) shouldBe Position(123, 456, "baz", "foo", "bar")
    pos.permute(ListSet(4, 1, 5, 3, 2).sized(5).get) shouldBe Position(123, "baz", 456, "foo", "bar")
    pos.permute(ListSet(4, 2, 1, 3, 5).sized(5).get) shouldBe Position("bar", 123, 456, "foo", "baz")
    pos.permute(ListSet(4, 2, 1, 5, 3).sized(5).get) shouldBe Position("bar", 123, "baz", "foo", 456)
    pos.permute(ListSet(4, 2, 3, 1, 5).sized(5).get) shouldBe Position(456, 123, "bar", "foo", "baz")
    pos.permute(ListSet(4, 2, 3, 5, 1).sized(5).get) shouldBe Position("baz", 123, "bar", "foo", 456)
    pos.permute(ListSet(4, 2, 5, 1, 3).sized(5).get) shouldBe Position(456, 123, "baz", "foo", "bar")
    pos.permute(ListSet(4, 2, 5, 3, 1).sized(5).get) shouldBe Position("baz", 123, 456, "foo", "bar")
    pos.permute(ListSet(4, 3, 1, 2, 5).sized(5).get) shouldBe Position("bar", 456, 123, "foo", "baz")
    pos.permute(ListSet(4, 3, 1, 5, 2).sized(5).get) shouldBe Position("bar", "baz", 123, "foo", 456)
    pos.permute(ListSet(4, 3, 2, 1, 5).sized(5).get) shouldBe Position(456, "bar", 123, "foo", "baz")
    pos.permute(ListSet(4, 3, 2, 5, 1).sized(5).get) shouldBe Position("baz", "bar", 123, "foo", 456)
    pos.permute(ListSet(4, 3, 5, 1, 2).sized(5).get) shouldBe Position(456, "baz", 123, "foo", "bar")
    pos.permute(ListSet(4, 3, 5, 2, 1).sized(5).get) shouldBe Position("baz", 456, 123, "foo", "bar")
    pos.permute(ListSet(4, 5, 1, 2, 3).sized(5).get) shouldBe Position("bar", 456, "baz", "foo", 123)
    pos.permute(ListSet(4, 5, 1, 3, 2).sized(5).get) shouldBe Position("bar", "baz", 456, "foo", 123)
    pos.permute(ListSet(4, 5, 2, 1, 3).sized(5).get) shouldBe Position(456, "bar", "baz", "foo", 123)
    pos.permute(ListSet(4, 5, 2, 3, 1).sized(5).get) shouldBe Position("baz", "bar", 456, "foo", 123)
    pos.permute(ListSet(4, 5, 3, 1, 2).sized(5).get) shouldBe Position(456, "baz", "bar", "foo", 123)
    pos.permute(ListSet(4, 5, 3, 2, 1).sized(5).get) shouldBe Position("baz", 456, "bar", "foo", 123)

    pos.permute(ListSet(5, 1, 2, 3, 4).sized(5).get) shouldBe Position(123, "bar", 456, "baz", "foo")
    pos.permute(ListSet(5, 1, 2, 4, 3).sized(5).get) shouldBe Position(123, "bar", "baz", 456, "foo")
    pos.permute(ListSet(5, 1, 3, 2, 4).sized(5).get) shouldBe Position(123, 456, "bar", "baz", "foo")
    pos.permute(ListSet(5, 1, 3, 4, 2).sized(5).get) shouldBe Position(123, "baz", "bar", 456, "foo")
    pos.permute(ListSet(5, 1, 4, 2, 3).sized(5).get) shouldBe Position(123, 456, "baz", "bar", "foo")
    pos.permute(ListSet(5, 1, 4, 3, 2).sized(5).get) shouldBe Position(123, "baz", 456, "bar", "foo")
    pos.permute(ListSet(5, 2, 1, 3, 4).sized(5).get) shouldBe Position("bar", 123, 456, "baz", "foo")
    pos.permute(ListSet(5, 2, 1, 4, 3).sized(5).get) shouldBe Position("bar", 123, "baz", 456, "foo")
    pos.permute(ListSet(5, 2, 3, 1, 4).sized(5).get) shouldBe Position(456, 123, "bar", "baz", "foo")
    pos.permute(ListSet(5, 2, 3, 4, 1).sized(5).get) shouldBe Position("baz", 123, "bar", 456, "foo")
    pos.permute(ListSet(5, 2, 4, 1, 3).sized(5).get) shouldBe Position(456, 123, "baz", "bar", "foo")
    pos.permute(ListSet(5, 2, 4, 3, 1).sized(5).get) shouldBe Position("baz", 123, 456, "bar", "foo")
    pos.permute(ListSet(5, 3, 1, 2, 4).sized(5).get) shouldBe Position("bar", 456, 123, "baz", "foo")
    pos.permute(ListSet(5, 3, 1, 4, 2).sized(5).get) shouldBe Position("bar", "baz", 123, 456, "foo")
    pos.permute(ListSet(5, 3, 2, 1, 4).sized(5).get) shouldBe Position(456, "bar", 123, "baz", "foo")
    pos.permute(ListSet(5, 3, 2, 4, 1).sized(5).get) shouldBe Position("baz", "bar", 123, 456, "foo")
    pos.permute(ListSet(5, 3, 4, 1, 2).sized(5).get) shouldBe Position(456, "baz", 123, "bar", "foo")
    pos.permute(ListSet(5, 3, 4, 2, 1).sized(5).get) shouldBe Position("baz", 456, 123, "bar", "foo")
    pos.permute(ListSet(5, 4, 1, 2, 3).sized(5).get) shouldBe Position("bar", 456, "baz", 123, "foo")
    pos.permute(ListSet(5, 4, 1, 3, 2).sized(5).get) shouldBe Position("bar", "baz", 456, 123, "foo")
    pos.permute(ListSet(5, 4, 2, 1, 3).sized(5).get) shouldBe Position(456, "bar", "baz", 123, "foo")
    pos.permute(ListSet(5, 4, 2, 3, 1).sized(5).get) shouldBe Position("baz", "bar", 456, 123, "foo")
    pos.permute(ListSet(5, 4, 3, 1, 2).sized(5).get) shouldBe Position(456, "baz", "bar", 123, "foo")
    pos.permute(ListSet(5, 4, 3, 2, 1).sized(5).get) shouldBe Position("baz", 456, "bar", 123, "foo")
  }

  it should "remove" in {
    pos.remove(_1) shouldBe Position(123, "bar", 456, "baz")
    pos.remove(_2) shouldBe Position("foo", "bar", 456, "baz")
    pos.remove(_3) shouldBe Position("foo", 123, 456, "baz")
    pos.remove(_4) shouldBe Position("foo", 123, "bar", "baz")
    pos.remove(_5) shouldBe Position("foo", 123, "bar", 456)
  }

  it should "melt" in {
    pos.melt(_1, _2, merge) shouldBe Position("123|foo", "bar", 456, "baz")
    pos.melt(_1, _3, merge) shouldBe Position(123, "bar|foo", 456, "baz")
    pos.melt(_1, _4, merge) shouldBe Position(123, "bar", "456|foo", "baz")
    pos.melt(_1, _5, merge) shouldBe Position(123, "bar", 456, "baz|foo")

    pos.melt(_2, _1, merge) shouldBe Position("foo|123", "bar", 456, "baz")
    pos.melt(_2, _3, merge) shouldBe Position("foo", "bar|123", 456, "baz")
    pos.melt(_2, _4, merge) shouldBe Position("foo", "bar", "456|123", "baz")
    pos.melt(_2, _5, merge) shouldBe Position("foo", "bar", 456, "baz|123")

    pos.melt(_3, _1, merge) shouldBe Position("foo|bar", 123, 456, "baz")
    pos.melt(_3, _2, merge) shouldBe Position("foo", "123|bar", 456, "baz")
    pos.melt(_3, _4, merge) shouldBe Position("foo", 123, "456|bar", "baz")
    pos.melt(_3, _5, merge) shouldBe Position("foo", 123, 456, "baz|bar")

    pos.melt(_4, _1, merge) shouldBe Position("foo|456", 123, "bar", "baz")
    pos.melt(_4, _2, merge) shouldBe Position("foo", "123|456", "bar", "baz")
    pos.melt(_4, _3, merge) shouldBe Position("foo", 123, "bar|456", "baz")
    pos.melt(_4, _5, merge) shouldBe Position("foo", 123, "bar", "baz|456")

    pos.melt(_5, _1, merge) shouldBe Position("foo|baz", 123, "bar", 456)
    pos.melt(_5, _2, merge) shouldBe Position("foo", "123|baz", "bar", 456)
    pos.melt(_5, _3, merge) shouldBe Position("foo", 123, "bar|baz", 456)
    pos.melt(_5, _4, merge) shouldBe Position("foo", 123, "bar", "456|baz")
  }
}

trait TestPositions extends TestGrimlock {

  val data = List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F").zipWithIndex

  val result1 = data.map { case (s, i) => Position(s) }.sorted
  val result2 = data.map { case (s, i) => Position(s) }.sorted
  val result3 = data.map { case (s, i) => Position(i) }.sorted
  val result4 = data.map { case (s, i) => Position(i) }.sorted
  val result5 = data.map { case (s, i) => Position(s) }.sorted
  val result6 = data.map { case (s, i) => Position(s) }.sorted
  val result7 = data.map { case (s, i) => Position(i) }.sorted
  val result8 = data.map { case (s, i) => Position(i + 1) }.sorted
  val result9 = data.map { case (s, i) => Position(i, i + 1) }.sorted
  val result10 = data.map { case (s, i) => Position(s, i + 1) }.sorted
  val result11 = data.map { case (s, i) => Position(s, i) }.sorted
  val result12 = data.map { case (s, i) => Position(s) }.sorted
  val result13 = data.map { case (s, i) => Position(i) }.sorted
  val result14 = data.map { case (s, i) => Position(i + 1) }.sorted
  val result15 = data.map { case (s, i) => Position(i + 2) }.sorted
  val result16 = data.map { case (s, i) => Position(i, i + 1, i + 2) }.sorted
  val result17 = data.map { case (s, i) => Position(s, i + 1, i + 2) }.sorted
  val result18 = data.map { case (s, i) => Position(s, i, i + 2) }.sorted
  val result19 = data.map { case (s, i) => Position(s, i, i + 1) }.sorted
  val result20 = data.map { case (s, i) => Position(s) }.sorted
  val result21 = data.map { case (s, i) => Position(i) }.sorted
  val result22 = data.map { case (s, i) => Position(i + 1) }.sorted
  val result23 = data.map { case (s, i) => Position(i + 2) }.sorted
  val result24 = data.map { case (s, i) => Position(i + 3) }.sorted
  val result25 = data.map { case (s, i) => Position(i, i + 1, i + 2, i + 3) }.sorted
  val result26 = data.map { case (s, i) => Position(s, i + 1, i + 2, i + 3) }.sorted
  val result27 = data.map { case (s, i) => Position(s, i, i + 2, i + 3) }.sorted
  val result28 = data.map { case (s, i) => Position(s, i, i + 1, i + 3) }.sorted
  val result29 = data.map { case (s, i) => Position(s, i, i + 1, i + 2) }.sorted
}

class TestScaldingPositions extends TestPositions {

  "A Positions of Position1D" should "return its _1 over names" in {
    toPipe(data.map { case (s, i) => Position(s) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its _1 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result2
  }

  it should "return its _2 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its _1 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result4
  }

  it should "return its _2 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its _1 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result6
  }

  it should "return its _2 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its _3 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result8
  }

  it should "return its _1 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its _2 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result10
  }

  it should "return its _3 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its _1 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result12
  }

  it should "return its _2 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its _3 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result14
  }

  it should "return its _4 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_4), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its _1 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result16
  }

  it should "return its _2 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its _3 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result18
  }

  it should "return its _4 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its _1 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result20
  }

  it should "return its _2 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its _3 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result22
  }

  it should "return its _4 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_4), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its _5 over names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_5), Default())
      .toList.sorted shouldBe result24
  }

  it should "return its _1 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its _2 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result26
  }

  it should "return its _3 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its _4 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result28
  }

  it should "return its _5 along names" in {
    toPipe(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_5), Default())
      .toList.sorted shouldBe result29
  }
}

class TestSparkPositions extends TestPositions {

  "A Positions of Position1D" should "return its _1 over names" in {
    toRDD(data.map { case (s, i) => Position(s) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its _1 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i) })
      .names(Over(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result2
  }

  it should "return its _2 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its _1 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i) })
      .names(Along(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result4
  }

  it should "return its _2 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its _1 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result6
  }

  it should "return its _2 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its _3 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_3), Default(Reducers(12)))
      .toList.sorted shouldBe result8
  }

  it should "return its _1 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its _2 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_2), Default(Reducers(12)))
      .toList.sorted shouldBe result10
  }

  it should "return its _3 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its _1 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result12
  }

  it should "return its _2 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its _3 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_3), Default(Reducers(12)))
      .toList.sorted shouldBe result14
  }

  it should "return its _4 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_4), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its _1 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result16
  }

  it should "return its _2 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its _3 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_3), Default(Reducers(12)))
      .toList.sorted shouldBe result18
  }

  it should "return its _4 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its _1 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_1), Default(Reducers(12)))
      .toList.sorted shouldBe result20
  }

  it should "return its _2 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its _3 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_3), Default(Reducers(12)))
      .toList.sorted shouldBe result22
  }

  it should "return its _4 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_4), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its _5 over names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_5), Default(Reducers(12)))
      .toList.sorted shouldBe result24
  }

  it should "return its _1 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its _2 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_2), Default(Reducers(12)))
      .toList.sorted shouldBe result26
  }

  it should "return its _3 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its _4 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_4), Default(Reducers(12)))
      .toList.sorted shouldBe result28
  }

  it should "return its _5 along names" in {
    toRDD(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_5), Default())
      .toList.sorted shouldBe result29
  }
}

