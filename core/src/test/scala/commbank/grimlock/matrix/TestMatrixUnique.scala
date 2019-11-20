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
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixUnique extends TestMatrix {
  val result1 = List(
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result2 = List(
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Content(ContinuousSchema[Double](), 12.56),
    Content(ContinuousSchema[Double](), 6.28),
    Content(
      DateSchema[java.util.Date](),
      DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
    ),
    Content(DiscreteSchema[Long](), 19L),
    Content(NominalSchema[String](), "9.42"),
    Content(OrdinalSchema[Long](), 19L),
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result4 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19L)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19L)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56")))

  val result5 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19L)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19L)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result6 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19L)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19L)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result7 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19L)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19L)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Content(ContinuousSchema[Double](), 12.56),
    Content(ContinuousSchema[Double](), 6.28),
    Content(
      DateSchema[java.util.Date](),
      DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
    ),
    Content(DiscreteSchema[Long](), 19L),
    Content(NominalSchema[String](), "9.42"),
    Content(OrdinalSchema[Long](), 19L),
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result9 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19L)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19L)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    (Position(2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    (Position(3, "xyz"), Content(NominalSchema[String](), "9.42")),
    (Position(3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    (
      Position(4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result11 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19L)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19L)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result12 = List(
    (Position("bar", "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19L)),
    (Position("bar", "xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19L)),
    (Position("baz", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo", "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo", "xyz"), Content(NominalSchema[String](), "9.42")),
    (Position("foo", "xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result13 = List(
    (Position("xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("xyz"), Content(DiscreteSchema[Long](), 19L)),
    (Position("xyz"), Content(NominalSchema[String](), "9.42")),
    (Position("xyz"), Content(OrdinalSchema[Long](), 19L)),
    (Position("xyz"), Content(OrdinalSchema[String](), "12.56")),
    (Position("xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position("xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position("xyz"), Content(OrdinalSchema[String](), "9.42"))
  )

  val result14 = List(
    (Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    (Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    (Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    (Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    (Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    (Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    (Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    (
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixUnique extends TestMatrixUnique with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.unique" should "return its content in 1D" in {
    toU(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toU(data1)
      .uniqueByPosition(Over(_0), Default())
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toU(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_0), Default())
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_0), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toU(data3)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_0), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_0), Default())
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_2), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_2), Default())
      .toList.sortBy(_.toString) shouldBe result14
  }
}

class TestScaldingMatrixUnique extends TestMatrixUnique with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.unique" should "return its content in 1D" in {
    toU(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toU(data1)
      .uniqueByPosition(Over(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toU(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_0), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toU(data3)
      .unique(Default(12))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_0), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_2), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

class TestSparkMatrixUnique extends TestMatrixUnique with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.unique" should "return its content in 1D" in {
    toU(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toU(data1)
      .uniqueByPosition(Over(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toU(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_0), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toU(data2)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toU(data2)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toU(data3)
      .unique(Default(12))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_0), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_0), Default(12))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toU(data3)
      .uniqueByPosition(Over(_2), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toU(data3)
      .uniqueByPosition(Along(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

