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

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixContract extends TestMatrix {
  val result1 = List(
    Cell(Position("1.bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("1.baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("1.foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("1.qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("2.bar"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("2.baz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("2.foo"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("3.bar"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("3.foo"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("4.foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result2 = List(
    Cell(Position("bar.1"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.3"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.1"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.2"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.1"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.2"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.3"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.4"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.1"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position(1, "xyz.bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position(1, "xyz.baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position(1, "xyz.foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position(1, "xyz.qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2, "xyz.bar"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2, "xyz.baz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position(2, "xyz.foo"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "xyz.bar"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position(3, "xyz.foo"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4, "xyz.foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result4 = List(
    Cell(Position("bar", "xyz.1"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "xyz.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "xyz.3"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "xyz.1"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "xyz.2"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", "xyz.1"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "xyz.2"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "xyz.3"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "xyz.4"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "xyz.1"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar.xyz", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.xyz", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.xyz", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.xyz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.xyz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.xyz", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.xyz", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.xyz", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.xyz", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.xyz", 1), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixContract extends TestMatrixContract with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.contract" should "return its first melted data in 2D" in {
    toU(data2)
      .contract(_0, _1, Value.concatenate[Value[Int], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toU(data2)
      .contract(_1, _0, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toU(data3)
      .contract(_0, _2, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toU(data3)
      .contract(_1, _2, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toU(data3)
      .contract(_2, _0, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestScaldingMatrixContract extends TestMatrixContract with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.contract" should "return its first melted data in 2D" in {
    toU(data2)
      .contract(_0, _1, Value.concatenate[Value[Int], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toU(data2)
      .contract(_1, _0, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toU(data3)
      .contract(_0, _2, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toU(data3)
      .contract(_1, _2, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toU(data3)
      .contract(_2, _0, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestSparkMatrixContract extends TestMatrixContract with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.contract" should "return its first melted data in 2D" in {
    toU(data2)
      .contract(_0, _1, Value.concatenate[Value[Int], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toU(data2)
      .contract(_1, _0, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toU(data3)
      .contract(_0, _2, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toU(data3)
      .contract(_1, _2, Value.concatenate[Value[String], Value[Int]]("."))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toU(data3)
      .contract(_2, _0, Value.concatenate[Value[String], Value[String]]("."))
      .toList.sortBy(_.position) shouldBe result5
  }
}

