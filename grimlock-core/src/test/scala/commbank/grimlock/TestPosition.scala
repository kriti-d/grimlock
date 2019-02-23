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

import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2, _3, _4 }

trait TestPosition extends TestGrimlock {
  def merge[VI <: Value[_], VD <: Value[_]](i: VI, d: VD): String = i.toShortString + "|" + d.toShortString
}

class TestPosition0D extends TestPosition {
  val pos = Position()

  "A Position0D" should "return its short string" in {
    pos.toShortString("|") shouldBe ""
  }

  it should "have coordinates" in {
    pos.asList shouldBe List()
  }

  it should "compare" in {
    pos.compare(pos) shouldBe 0
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
    pos.asList shouldBe List(StringValue("foo"))
  }

  it should "return its coordinates" in {
    pos(_0) shouldBe StringValue("foo")
  }

  it should "compare" in {
    pos.compare(Position("abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz")) should be < 0
  }

  it should "be updated" in {
    pos.update(_0, 123) shouldBe Position(123)
  }

  it should "remove" in {
    pos.remove(_0) shouldBe Position()
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
    pos.asList shouldBe List(StringValue("foo"), IntValue(123))
  }

  it should "return its coordinates" in {
    pos(_0) shouldBe StringValue("foo")
    pos(_1) shouldBe IntValue(123)
  }

  it should "compare" in {
    pos.compare(Position("abc", 456)) should be > 0
    pos.compare(Position("foo", 1)) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1)) should be < 0
    pos.compare(Position("foo", 789)) should be < 0
  }

  it should "be updated" in {
    pos.update(_0, 123) shouldBe Position(123, 123)
    pos.update(_1, "xyz") shouldBe Position("foo", "xyz")
  }

  it should "remove" in {
    pos.remove(_0) shouldBe Position(123)
    pos.remove(_1) shouldBe Position("foo")
  }

  it should "melt" in {
    pos.melt(_0, _1, merge[Value[Int], Value[String]]) shouldBe Position("123|foo")
    pos.melt(_1, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|123")
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
    pos.asList shouldBe List(StringValue("foo"), IntValue(123), StringValue("bar"))
  }

  it should "return its coordinates" in {
    pos(_0) shouldBe StringValue("foo")
    pos(_1) shouldBe IntValue(123)
    pos(_2) shouldBe StringValue("bar")
  }

  it should "compare" in {
    pos.compare(Position("abc", 456, "xyz")) should be > 0
    pos.compare(Position("foo", 1, "xyz")) should be > 0
    pos.compare(Position("foo", 123, "abc")) should be > 0
    pos.compare(pos) shouldBe 0
    pos.compare(Position("xyz", 1, "abc")) should be < 0
    pos.compare(Position("foo", 789, "abc")) should be < 0
    pos.compare(Position("foo", 123, "xyz")) should be < 0
  }

  it should "be updated" in {
    pos.update(_0, 123) shouldBe Position(123, 123, "bar")
    pos.update(_1, "xyz") shouldBe Position("foo", "xyz", "bar")
    pos.update(_2, 456) shouldBe Position("foo", 123, 456)
  }

  it should "remove" in {
    pos.remove(_0) shouldBe Position(123, "bar")
    pos.remove(_1) shouldBe Position("foo", "bar")
    pos.remove(_2) shouldBe Position("foo", 123)
  }

  it should "melt" in {
    pos.melt(_0, _1, merge[Value[Int], Value[String]]) shouldBe Position("123|foo", "bar")
    pos.melt(_0, _2, merge[Value[String], Value[String]]) shouldBe Position(123, "bar|foo")

    pos.melt(_1, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|123", "bar")
    pos.melt(_1, _2, merge[Value[String], Value[Int]]) shouldBe Position("foo", "bar|123")

    pos.melt(_2, _0, merge[Value[String], Value[String]]) shouldBe Position("foo|bar", 123)
    pos.melt(_2, _1, merge[Value[Int], Value[String]]) shouldBe Position("foo", "123|bar")
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
    pos.asList shouldBe List(StringValue("foo"), IntValue(123), StringValue("bar"), IntValue(456))
  }

  it should "return its coordinates" in {
    pos(_0) shouldBe StringValue("foo")
    pos(_1) shouldBe IntValue(123)
    pos(_2) shouldBe StringValue("bar")
    pos(_3) shouldBe IntValue(456)
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
  }

  it should "be updated" in {
    pos.update(_0, 123) shouldBe Position(123, 123, "bar", 456)
    pos.update(_1, "xyz") shouldBe Position("foo", "xyz", "bar", 456)
    pos.update(_2, 456) shouldBe Position("foo", 123, 456, 456)
    pos.update(_3, "abc") shouldBe Position("foo", 123, "bar", "abc")
  }

  it should "remove" in {
    pos.remove(_0) shouldBe Position(123, "bar", 456)
    pos.remove(_1) shouldBe Position("foo", "bar", 456)
    pos.remove(_2) shouldBe Position("foo", 123, 456)
    pos.remove(_3) shouldBe Position("foo", 123, "bar")
  }

  it should "melt" in {
    pos.melt(_0, _1, merge[Value[Int], Value[String]]) shouldBe Position("123|foo", "bar", 456)
    pos.melt(_0, _2, merge[Value[String], Value[String]]) shouldBe Position(123, "bar|foo", 456)
    pos.melt(_0, _3, merge[Value[Int], Value[String]]) shouldBe Position(123, "bar", "456|foo")

    pos.melt(_1, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|123", "bar", 456)
    pos.melt(_1, _2, merge[Value[String], Value[Int]]) shouldBe Position("foo", "bar|123", 456)
    pos.melt(_1, _3, merge[Value[Int], Value[Int]]) shouldBe Position("foo", "bar", "456|123")

    pos.melt(_2, _0, merge[Value[String], Value[String]]) shouldBe Position("foo|bar", 123, 456)
    pos.melt(_2, _1, merge[Value[Int], Value[String]]) shouldBe Position("foo", "123|bar", 456)
    pos.melt(_2, _3, merge[Value[Int], Value[String]]) shouldBe Position("foo", 123, "456|bar")

    pos.melt(_3, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|456", 123, "bar")
    pos.melt(_3, _1, merge[Value[Int], Value[Int]]) shouldBe Position("foo", "123|456", "bar")
    pos.melt(_3, _2, merge[Value[String], Value[Int]]) shouldBe Position("foo", 123, "bar|456")
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
    pos.asList shouldBe List(StringValue("foo"), IntValue(123), StringValue("bar"), IntValue(456), StringValue("baz"))
  }

  it should "return its coordinates" in {
    pos(_0) shouldBe StringValue("foo")
    pos(_1) shouldBe IntValue(123)
    pos(_2) shouldBe StringValue("bar")
    pos(_3) shouldBe IntValue(456)
    pos(_4) shouldBe StringValue("baz")
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
  }

  it should "be updated" in {
    pos.update(_0, 123) shouldBe Position(123, 123, "bar", 456, "baz")
    pos.update(_1, "xyz") shouldBe Position("foo", "xyz", "bar", 456, "baz")
    pos.update(_2, 456) shouldBe Position("foo", 123, 456, 456, "baz")
    pos.update(_3, "abc") shouldBe Position("foo", 123, "bar", "abc", "baz")
    pos.update(_4, 789) shouldBe Position("foo", 123, "bar", 456, 789)
  }

  it should "remove" in {
    pos.remove(_0) shouldBe Position(123, "bar", 456, "baz")
    pos.remove(_1) shouldBe Position("foo", "bar", 456, "baz")
    pos.remove(_2) shouldBe Position("foo", 123, 456, "baz")
    pos.remove(_3) shouldBe Position("foo", 123, "bar", "baz")
    pos.remove(_4) shouldBe Position("foo", 123, "bar", 456)
  }

  it should "melt" in {
    pos.melt(_0, _1, merge[Value[Int], Value[String]]) shouldBe Position("123|foo", "bar", 456, "baz")
    pos.melt(_0, _2, merge[Value[String], Value[String]]) shouldBe Position(123, "bar|foo", 456, "baz")
    pos.melt(_0, _3, merge[Value[Int], Value[String]]) shouldBe Position(123, "bar", "456|foo", "baz")
    pos.melt(_0, _4, merge[Value[String], Value[String]]) shouldBe Position(123, "bar", 456, "baz|foo")

    pos.melt(_1, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|123", "bar", 456, "baz")
    pos.melt(_1, _2, merge[Value[String], Value[Int]]) shouldBe Position("foo", "bar|123", 456, "baz")
    pos.melt(_1, _3, merge[Value[Int], Value[Int]]) shouldBe Position("foo", "bar", "456|123", "baz")
    pos.melt(_1, _4, merge[Value[String], Value[Int]]) shouldBe Position("foo", "bar", 456, "baz|123")

    pos.melt(_2, _0, merge[Value[String], Value[String]]) shouldBe Position("foo|bar", 123, 456, "baz")
    pos.melt(_2, _1, merge[Value[Int], Value[String]]) shouldBe Position("foo", "123|bar", 456, "baz")
    pos.melt(_2, _3, merge[Value[Int], Value[String]]) shouldBe Position("foo", 123, "456|bar", "baz")
    pos.melt(_2, _4, merge[Value[String], Value[String]]) shouldBe Position("foo", 123, 456, "baz|bar")

    pos.melt(_3, _0, merge[Value[String], Value[Int]]) shouldBe Position("foo|456", 123, "bar", "baz")
    pos.melt(_3, _1, merge[Value[Int], Value[Int]]) shouldBe Position("foo", "123|456", "bar", "baz")
    pos.melt(_3, _2, merge[Value[String], Value[Int]]) shouldBe Position("foo", 123, "bar|456", "baz")
    pos.melt(_3, _4, merge[Value[String], Value[Int]]) shouldBe Position("foo", 123, "bar", "baz|456")

    pos.melt(_4, _0, merge[Value[String], Value[String]]) shouldBe Position("foo|baz", 123, "bar", 456)
    pos.melt(_4, _1, merge[Value[Int], Value[String]]) shouldBe Position("foo", "123|baz", "bar", 456)
    pos.melt(_4, _2, merge[Value[String], Value[String]]) shouldBe Position("foo", 123, "bar|baz", 456)
    pos.melt(_4, _3, merge[Value[Int], Value[String]]) shouldBe Position("foo", 123, "bar", "456|baz")
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

class TestScalaPositions extends TestPositions with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Positions of Position1D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result2
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result4
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result6
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result8
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result10
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result12
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result14
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result16
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result18
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result20
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_2), Default())
      .toList.sorted shouldBe result22
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its _4 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_4), Default())
      .toList.sorted shouldBe result24
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result26
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result28
  }

  it should "return its _4 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result29
  }
}

class TestScaldingPositions extends TestPositions with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Positions of Position1D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result2
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_0), Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result6
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result8
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_1), Default(12))
      .toList.sorted shouldBe result10
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result12
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result14
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_0), Default(12))
      .toList.sorted shouldBe result16
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_2), Default(12))
      .toList.sorted shouldBe result18
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result20
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result22
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its _4 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_4), Default(12))
      .toList.sorted shouldBe result24
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_1), Default(12))
      .toList.sorted shouldBe result26
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_3), Default(12))
      .toList.sorted shouldBe result28
  }

  it should "return its _4 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result29
  }
}

class TestSparkPositions extends TestPositions with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Positions of Position1D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s) })
      .names(Over(_0), Default())
      .toList.sorted shouldBe result1
  }

  "A Positions of Position2D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result2
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result3
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_0), Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result5
  }

  "A Positions of Position3D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result6
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result7
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result8
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result9
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_1), Default(12))
      .toList.sorted shouldBe result10
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result11
  }

  "A Positions of Position4D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result12
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result13
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result14
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result15
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_0), Default(12))
      .toList.sorted shouldBe result16
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_1), Default())
      .toList.sorted shouldBe result17
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_2), Default(12))
      .toList.sorted shouldBe result18
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2) })
      .names(Along(_3), Default())
      .toList.sorted shouldBe result19
  }

  "A Positions of Position5D" should "return its _0 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe result20
  }

  it should "return its _1 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_1), Default())
      .toList.sorted shouldBe result21
  }

  it should "return its _2 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe result22
  }

  it should "return its _3 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_3), Default())
      .toList.sorted shouldBe result23
  }

  it should "return its _4 over names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Over(_4), Default(12))
      .toList.sorted shouldBe result24
  }

  it should "return its _0 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_0), Default())
      .toList.sorted shouldBe result25
  }

  it should "return its _1 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_1), Default(12))
      .toList.sorted shouldBe result26
  }

  it should "return its _2 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_2), Default())
      .toList.sorted shouldBe result27
  }

  it should "return its _3 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_3), Default(12))
      .toList.sorted shouldBe result28
  }

  it should "return its _4 along names" in {
    toU(data.map { case (s, i) => Position(s, i, i + 1, i + 2, i + 3) })
      .names(Along(_4), Default())
      .toList.sorted shouldBe result29
  }
}

